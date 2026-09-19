mod support;

use pqbench::parquet_helpers::{default_metadata_parser, MetadataParser};
use pqbench::table::delta::read_local;
use serde_json::json;
use support::{metadata, remove, write_parquet, Fixture};

#[tokio::test]
async fn resolves_versions_and_weights_columns_by_total_rows() {
    let fixture = Fixture::new();
    let previous = read_local(fixture.path(), Some(0)).await.unwrap();
    assert_eq!(previous.version, 0);
    assert_eq!(previous.file_count, 2);
    assert_eq!(previous.physical_rows, 7);

    let latest = read_local(fixture.path(), None).await.unwrap();
    assert_eq!(latest.version, 1);
    assert_eq!(latest.file_count, 2);
    assert_eq!(latest.physical_rows, 14);
    assert_eq!(latest.partition_columns, ["part"]);
    assert_eq!(latest.columns.len(), 1);
    assert_eq!(latest.columns[0].path, "id");
    let mut bytes = 0;
    let mut uncompressed = 0;
    let mut file_bytes = 0;
    for relative in ["part=b/kept.parquet", "part=a/added.parquet"] {
        let path = fixture.path().join(relative);
        let mass = default_metadata_parser().read_masses(&path).unwrap();
        bytes += mass.columns[0].bytes;
        uncompressed += mass.columns[0].uncompressed_bytes;
        file_bytes += std::fs::metadata(path).unwrap().len();
    }
    assert_eq!(latest.file_bytes, file_bytes);
    assert_eq!(latest.compressed_column_bytes, bytes);
    assert_eq!(latest.uncompressed_column_bytes, uncompressed);
    assert_eq!(latest.compressed_bytes_per_row(), bytes as f64 / 14.0);
    let report: serde_json::Value =
        serde_json::from_str(&pqbench::table::delta::json(&latest).unwrap()).unwrap();
    assert_eq!(report["physical_rows"], 14);
    assert_eq!(report["columns"][0]["compressed_bytes"], bytes);
    assert!(pqbench::table::delta::render(&latest).contains("physical rows: 14"));
}

#[tokio::test]
async fn ignores_tombstoned_and_untracked_files() {
    let fixture = Fixture::new();
    std::fs::write(
        fixture.path().join("part=a/old file.parquet"),
        b"not parquet",
    )
    .unwrap();
    std::fs::write(fixture.path().join("untracked.parquet"), b"not parquet").unwrap();
    assert_eq!(
        read_local(fixture.path(), None)
            .await
            .unwrap()
            .physical_rows,
        14
    );
    assert!(read_local(fixture.path(), Some(0)).await.is_err());
}

#[tokio::test]
async fn resolves_checkpoint_after_old_json_is_removed() {
    let fixture = Fixture::new();
    let url = url::Url::from_directory_path(fixture.path()).unwrap();
    let table = deltalake::DeltaTableBuilder::from_url(url)
        .unwrap()
        .load()
        .await
        .unwrap();
    deltalake::protocol::checkpoints::create_checkpoint(&table, None)
        .await
        .unwrap();
    std::fs::remove_file(fixture.path().join("_delta_log/00000000000000000000.json")).unwrap();
    let report = read_local(fixture.path(), Some(1)).await.unwrap();
    assert_eq!(report.physical_rows, 14);
    assert_eq!(report.file_count, 2);
}

#[tokio::test]
async fn empty_snapshot_has_zero_totals() {
    let fixture = Fixture::new();
    fixture.commit(
        2,
        &[
            remove("part=b/kept.parquet"),
            remove("part=a/added.parquet"),
        ],
    );
    let report = read_local(fixture.path(), None).await.unwrap();
    assert_eq!(report.version, 2);
    assert_eq!(report.file_count, 0);
    assert_eq!(report.physical_rows, 0);
    assert_eq!(report.file_bytes, 0);
    assert_eq!(report.compressed_bytes_per_row(), 0.0);
    assert!(report.columns.is_empty());
}

#[tokio::test]
async fn missing_or_changed_active_files_fail_without_partial_results() {
    let fixture = Fixture::new();
    let path = fixture.path().join("part=a/added.parquet");
    std::fs::write(&path, b"changed").unwrap();
    let error = read_local(fixture.path(), None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("size differs from log"), "{error}");
    std::fs::remove_file(path).unwrap();
    let error = read_local(fixture.path(), None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("cannot open active file"), "{error}");
}

#[tokio::test]
async fn rejects_missing_versions_and_non_tables() {
    let fixture = Fixture::new();
    assert!(read_local(fixture.path(), Some(99)).await.is_err());
    let empty = tempfile::tempdir().unwrap();
    let error = read_local(empty.path(), None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("missing _delta_log"), "{error}");
}

#[tokio::test]
async fn rejects_unsupported_column_mapping() {
    let fixture = Fixture::new();
    // A feature advertised without valid mapping metadata must be rejected by
    // either delta-rs protocol validation or our reporting capability check.
    fixture.commit(2, &[metadata(json!({"delta.columnMapping.mode": "name"}))]);
    assert!(read_local(fixture.path(), None).await.is_err());
}

#[tokio::test]
async fn rejects_external_data_paths() {
    let fixture = Fixture::new();
    let parent = fixture.path().parent().unwrap();
    let outside = tempfile::tempdir_in(parent).unwrap();
    write_parquet(&outside.path().join("outside.parquet"), 9);
    let mut add = fixture.add("part=a/added.parquet", "a", 9);
    add["add"]["path"] = json!(format!(
        "../{}/outside.parquet",
        outside.path().file_name().unwrap().to_string_lossy()
    ));
    fixture.commit(2, &[add]);
    let error = read_local(fixture.path(), None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("only relative data paths"), "{error}");
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_symlink_escape_from_table_directory() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let outside = tempfile::tempdir().unwrap();
    write_parquet(&outside.path().join("outside.parquet"), 9);
    symlink(outside.path(), fixture.path().join("escape")).unwrap();
    let mut add = fixture.add("part=a/added.parquet", "a", 9);
    add["add"]["path"] = json!("escape/outside.parquet");
    fixture.commit(2, &[add]);
    let error = read_local(fixture.path(), None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("outside the table directory"), "{error}");
}
