#![cfg(feature = "delta")]

mod support;

use pqbench::parquet_helpers::{default_metadata_parser, MetadataParser};
use pqbench::table::{self, LoadRequest, TableFormat};
use serde_json::json;
use support::{metadata, remove, write_parquet, Fixture};

fn load_request(uri: impl Into<String>, version: Option<u64>) -> LoadRequest {
    LoadRequest::new(uri, version, Default::default())
}

#[tokio::test]
async fn load_emits_every_json_commit_and_only_active_files() {
    let fixture = Fixture::new();
    let info = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(info.kind, "pqbench.table");
    assert_eq!(info.format, TableFormat::DELTA);
    assert_eq!(info.snapshot_version, 1);
    assert_eq!(info.log.len(), 2);
    assert!(info.log[1]
        .actions
        .iter()
        .any(|action| action.kind == "remove"));
    let mut paths: Vec<_> = info.files.iter().map(|file| file.path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(paths, ["part=a/added.parquet", "part=b/kept.parquet"]);
    let rows = pqbench::bytemass::bytemass(&pqbench::bytemass::BytemassRequest {
        inputs: info.files.iter().map(|file| file.uri.clone()).collect(),
        env: info.env.clone(),
    })
    .await
    .unwrap();
    let summary = pqbench::bytemass::aggregate(&rows).unwrap();
    assert_eq!(summary.num_rows, 14);
    assert_eq!(summary.file_count, 2);
}

#[tokio::test]
async fn load_selects_a_snapshot_and_bytemass_weights_columns() {
    let fixture = Fixture::new();
    let previous = table::load(&load_request(fixture.path().to_string_lossy(), Some(0)))
        .await
        .unwrap();
    assert_eq!(previous.snapshot_version, 0);
    assert_eq!(previous.files.len(), 2);

    let latest = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(latest.snapshot_version, 1);
    assert_eq!(latest.files.len(), 2);
    assert_eq!(latest.partition_columns, ["part"]);

    let rows = pqbench::bytemass::bytemass(&pqbench::bytemass::BytemassRequest {
        inputs: latest.files.iter().map(|file| file.uri.clone()).collect(),
        env: latest.env.clone(),
    })
    .await
    .unwrap();
    let summary = pqbench::bytemass::aggregate(&rows).unwrap();
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
    assert_eq!(summary.num_rows, 14);
    assert_eq!(summary.file_count, 2);
    assert_eq!(summary.columns.len(), 1);
    assert_eq!(summary.columns[0].path, "id");
    assert_eq!(
        latest.files.iter().map(|file| file.size).sum::<u64>(),
        file_bytes
    );
    assert_eq!(summary.columns[0].compressed_bytes, bytes);
    assert_eq!(summary.columns[0].uncompressed_bytes, uncompressed);
}

#[tokio::test]
async fn load_accepts_a_file_uri() {
    let fixture = Fixture::new();
    let uri = url::Url::from_directory_path(fixture.path()).unwrap();
    let info = table::load(&load_request(uri.as_str(), None))
        .await
        .unwrap();
    assert_eq!(info.snapshot_version, 1);
    assert_eq!(info.files.len(), 2);
    assert_eq!(info.partition_columns, ["part"]);
}

#[tokio::test]
async fn load_ignores_tombstoned_and_untracked_files() {
    let fixture = Fixture::new();
    std::fs::write(
        fixture.path().join("part=a/old file.parquet"),
        b"not parquet",
    )
    .unwrap();
    std::fs::write(fixture.path().join("untracked.parquet"), b"not parquet").unwrap();
    let latest = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(latest.files.len(), 2);
    assert!(latest
        .files
        .iter()
        .all(|file| file.path != "part=a/old file.parquet"));
    let rows = pqbench::bytemass::bytemass(&pqbench::bytemass::BytemassRequest {
        inputs: latest.files.iter().map(|file| file.uri.clone()).collect(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(pqbench::bytemass::aggregate(&rows).unwrap().num_rows, 14);
    let previous = table::load(&load_request(fixture.path().to_string_lossy(), Some(0)))
        .await
        .unwrap();
    assert_eq!(previous.files.len(), 2);
}

#[tokio::test]
async fn load_resolves_checkpoint_after_old_json_is_removed() {
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
    let info = table::load(&load_request(fixture.path().to_string_lossy(), Some(1)))
        .await
        .unwrap();
    assert_eq!(info.snapshot_version, 1);
    assert_eq!(info.files.len(), 2);
}

#[tokio::test]
async fn empty_snapshot_names_no_files() {
    let fixture = Fixture::new();
    fixture.commit(
        2,
        &[
            remove("part=b/kept.parquet"),
            remove("part=a/added.parquet"),
        ],
    );
    let info = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(info.snapshot_version, 2);
    assert!(info.files.is_empty());
}

#[tokio::test]
async fn load_records_log_size_and_bytemass_sees_a_changed_file() {
    let fixture = Fixture::new();
    let path = fixture.path().join("part=a/added.parquet");
    let expected = std::fs::metadata(&path).unwrap().len();
    std::fs::write(&path, b"changed").unwrap();
    let info = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    let added = info
        .files
        .iter()
        .find(|file| file.path == "part=a/added.parquet")
        .unwrap();
    assert_eq!(added.size, expected);
    let rows = pqbench::bytemass::bytemass(&pqbench::bytemass::BytemassRequest {
        inputs: vec![added.uri.clone()],
        ..Default::default()
    })
    .await;
    assert!(rows.is_err() || rows.unwrap().iter().any(|row| row.size != expected));
    std::fs::remove_file(path).unwrap();
    let info = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    let added = info
        .files
        .iter()
        .find(|file| file.path == "part=a/added.parquet")
        .unwrap();
    let error = pqbench::bytemass::bytemass(&pqbench::bytemass::BytemassRequest {
        inputs: vec![added.uri.clone()],
        ..Default::default()
    })
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(
        error.contains("cannot stat") || error.contains("cannot open"),
        "{error}"
    );
}

#[tokio::test]
async fn load_rejects_missing_versions_and_non_tables() {
    let fixture = Fixture::new();
    assert!(
        table::load(&load_request(fixture.path().to_string_lossy(), Some(99)))
            .await
            .is_err()
    );
    let empty = tempfile::tempdir().unwrap();
    let error = table::load(&load_request(empty.path().to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("unrecognized table format") || error.contains("cannot open table"),
        "{error}"
    );
}

#[tokio::test]
async fn load_does_not_treat_column_mapping_as_byte_mass() {
    let fixture = Fixture::new();
    fixture.commit(2, &[metadata(json!({"delta.columnMapping.mode": "name"}))]);
    if let Err(error) = table::load(&load_request(fixture.path().to_string_lossy(), None)).await {
        assert!(!error.to_string().contains("byte-mass"), "{error}");
    }
}

#[tokio::test]
async fn load_lists_files_that_carry_deletion_vectors() {
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
    let info = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .unwrap();
    assert!(info
        .files
        .iter()
        .any(|file| file.path == "part=a/added.parquet"));
}

#[tokio::test]
async fn load_rejects_external_data_paths() {
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
    let error = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("only relative data paths"), "{error}");
}

#[tokio::test]
async fn load_copies_env_onto_the_document() {
    let fixture = Fixture::new();
    let mut env = std::collections::BTreeMap::new();
    env.insert("AWS_REGION".into(), "us-east-1".into());
    let info = table::load(&LoadRequest::new(
        fixture.path().to_string_lossy(),
        None,
        env.clone(),
    ))
    .await
    .unwrap();
    assert_eq!(info.env, env);
}

#[tokio::test]
async fn load_reports_a_broken_snapshot() {
    let fixture = Fixture::new();
    fixture.commit(
        2,
        &[json!({"metaData": {
            "id": "967e1749-2635-481d-a114-897e027d7000",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}}
            ]}).to_string(),
            "partitionColumns": ["part"],
            "configuration": {},
            "createdTime": 0
        }})],
    );
    let error = table::load(&load_request(fixture.path().to_string_lossy(), None))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("Partition column"), "{error}");
}
