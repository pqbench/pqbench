#![cfg(feature = "delta")]

mod support;

use pqbench::parquet_helpers::{default_metadata_parser, MetadataParser};
use pqbench::table::delta::{delta, render_json, render_text, DeltaRequest};
use pqbench::table::{self, LoadRequest, TableFormat};
use serde_json::json;
use support::{metadata, remove, write_parquet, Fixture};

fn request(table: impl Into<String>, version: Option<u64>) -> DeltaRequest {
    DeltaRequest {
        table: table.into(),
        version,
    }
}

fn load_request(uri: impl Into<String>, version: Option<u64>) -> LoadRequest {
    LoadRequest {
        uri: uri.into(),
        version,
        env: Default::default(),
    }
}

#[tokio::test]
async fn load_emits_every_json_commit_and_only_active_files() {
    let fixture = Fixture::new();
    let info = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(info.kind, "pqbench.table");
    assert_eq!(info.format, TableFormat::Delta);
    assert_eq!(info.snapshot_version, 1);
    assert_eq!(info.log.len(), 2);
    assert!(info.log[1]
        .actions
        .iter()
        .any(|action| action.get("remove").is_some()));
    let mut paths: Vec<_> = info.files.iter().map(|file| file.path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(paths, ["part=a/added.parquet", "part=b/kept.parquet"]);
    let rows = pqbench::bytemass::bytemass(&pqbench::bytemass::BytemassRequest {
        inputs: info.files.iter().map(|file| file.uri.clone()).collect(),
    })
    .await
    .unwrap();
    let summary = pqbench::bytemass::aggregate(&rows).unwrap();
    let expected = delta(&request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(summary.num_rows, expected.physical_rows);
    assert_eq!(summary.file_count, expected.file_count);
}

#[tokio::test]
async fn resolves_versions_and_weights_columns_by_total_rows() {
    let fixture = Fixture::new();
    let previous = delta(&request(fixture.path().to_string_lossy(), Some(0)))
        .await
        .unwrap();
    assert_eq!(previous.version, 0);
    assert_eq!(previous.file_count, 2);
    assert_eq!(previous.physical_rows, 7);

    let latest = delta(&request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
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
    let report: serde_json::Value = serde_json::from_str(&render_json(&latest).unwrap()).unwrap();
    assert_eq!(report["physical_rows"], 14);
    assert_eq!(report["columns"][0]["compressed_bytes"], bytes);
    assert!(render_text(&latest).unwrap().contains("physical rows: 14"));
}

#[tokio::test]
async fn reads_a_table_uri_with_the_public_remote_api() {
    let fixture = Fixture::new();
    let uri = url::Url::from_directory_path(fixture.path()).unwrap();
    let report = delta(&request(uri.as_str(), None)).await.unwrap();

    assert_eq!(report.version, 1);
    assert_eq!(report.file_count, 2);
    assert_eq!(report.physical_rows, 14);
    assert_eq!(report.partition_columns, ["part"]);
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
        delta(&request(fixture.path().to_string_lossy(), None))
            .await
            .unwrap()
            .physical_rows,
        14
    );
    assert!(delta(&request(fixture.path().to_string_lossy(), Some(0)))
        .await
        .is_err());
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
    let report = delta(&request(fixture.path().to_string_lossy(), Some(1)))
        .await
        .unwrap();
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
    let report = delta(&request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
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
    let error = delta(&request(fixture.path().to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("size differs from log"), "{error}");
    std::fs::remove_file(path).unwrap();
    let error = delta(&request(fixture.path().to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("cannot open active file"), "{error}");
}

#[tokio::test]
async fn rejects_missing_versions_and_non_tables() {
    let fixture = Fixture::new();
    assert!(delta(&request(fixture.path().to_string_lossy(), Some(99)))
        .await
        .is_err());
    let empty = tempfile::tempdir().unwrap();
    let error = delta(&request(empty.path().to_string_lossy(), None))
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
    assert!(delta(&request(fixture.path().to_string_lossy(), None))
        .await
        .is_err());
}

#[tokio::test]
async fn rejects_deletion_vectors_before_reading_data_files() {
    let fixture = Fixture::new();
    let mut add = fixture.add("part=a/added.parquet", "a", 9);
    add["add"]["deletionVector"] = json!({
        "storageType": "u",
        "pathOrInlineDv": "deletion-vector.bin",
        "offset": 0,
        "sizeInBytes": 1,
        "cardinality": 1
    });
    fixture.commit(2, &[add]);
    let error = delta(&request(fixture.path().to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("deletion vectors are not supported"),
        "{error}"
    );
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
    let error = delta(&request(fixture.path().to_string_lossy(), None))
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
    let error = delta(&request(fixture.path().to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("outside the table directory"), "{error}");
}
