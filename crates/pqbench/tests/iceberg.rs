#![cfg(feature = "iceberg")]

mod common;
mod support;

use std::fs;
use std::path::Path;

use apache_avro::{Schema, Writer};
use futures_util::TryStreamExt;
use pqbench::table::{self, LoadRequest};
use pqbench::third_party::parquet::{default_metadata_parser, MetadataParser};
use pqbench::v1::TableFormat;
use serde::Serialize;
use support::write_parquet;
use url::Url;

fn load_request(uri: impl Into<String>, version: Option<u64>) -> LoadRequest {
    LoadRequest::new(uri, version)
}

#[derive(Serialize)]
struct ManifestListRow {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32,
}

#[derive(Serialize)]
struct ManifestRow {
    status: i32,
    data_file: ManifestDataFile,
}

#[derive(Serialize)]
struct ManifestDataFile {
    content: i32,
    file_path: String,
    file_format: String,
    partition: EmptyPartition,
    record_count: i64,
    #[serde(rename = "file_size_in_bytes")]
    file_size_bytes: i64,
}

#[derive(Serialize)]
struct EmptyPartition {}

const MANIFEST_LIST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int", "default": 0}
  ]
}"#;

const MANIFEST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "data_file", "type": {
      "type": "record",
      "name": "data_file",
      "fields": [
        {"name": "content", "type": "int", "default": 0},
        {"name": "file_path", "type": "string"},
        {"name": "file_format", "type": "string"},
        {"name": "partition", "type": {"type": "record", "name": "r102", "fields": []}},
        {"name": "record_count", "type": "long"},
        {"name": "file_size_in_bytes", "type": "long"}
      ]
    }}
  ]
}"#;

struct Fixture {
    // Held only to keep the temporary directory alive for the test's duration.
    // aipnaming: allow(aip-140/underscores)
    _directory: tempfile::TempDir,
    root: std::path::PathBuf,
    metadata: std::path::PathBuf,
    first_size: u64,
    second_size: u64,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_path_buf();
        let data = root.join("data");
        let metadata_dir = root.join("metadata");
        fs::create_dir_all(&data).unwrap();
        fs::create_dir_all(&metadata_dir).unwrap();

        let first = data.join("first.parquet");
        let second = data.join("second.parquet");
        write_parquet(&first, 3);
        write_parquet(&second, 5);
        let first_size = fs::metadata(&first).unwrap().len();
        let second_size = fs::metadata(&second).unwrap().len();

        let v0 = write_manifest(
            &metadata_dir.join("m0.avro"),
            &[data_entry(file_uri(&first), first_size, 3, 0)],
        );
        let v1 = write_manifest(
            &metadata_dir.join("m1.avro"),
            &[
                data_entry(file_uri(&first), first_size, 3, 0),
                data_entry(file_uri(&second), second_size, 5, 0),
            ],
        );
        let deletes = write_manifest(
            &metadata_dir.join("d1.avro"),
            &[delete_entry(file_uri(&data.join("deletes.parquet")))],
        );
        let list0 = write_manifest_list(&metadata_dir.join("snap-0.avro"), &[(&v0, 0)]);
        let list1 = write_manifest_list(
            &metadata_dir.join("snap-1.avro"),
            &[(&v1, 0), (&deletes, 1)],
        );

        let metadata = metadata_dir.join("v1.metadata.json");
        fs::write(
            &metadata,
            serde_json::to_vec_pretty(&serde_json::json!({
                "format-version": 2,
                "table-uuid": "11111111-1111-1111-1111-111111111111",
                "location": dir_uri(&root),
                "last-updated-ms": 0,
                "last-column-id": 1,
                "current-schema-id": 0,
                "schemas": [{"type":"struct","schema-id":0,"fields":[
                    {"id":1,"name":"id","required":true,"type":"long"}
                ]}],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id":0,"fields":[]}],
                "last-partition-id": 999,
                "current-snapshot-id": 1,
                "snapshots": [
                    {
                        "snapshot-id": 0,
                        "sequence-number": 0,
                        "timestamp-ms": 0,
                        "manifest-list": file_uri(&list0),
                        "schema-id": 0
                    },
                    {
                        "snapshot-id": 1,
                        "parent-snapshot-id": 0,
                        "sequence-number": 1,
                        "timestamp-ms": 1,
                        "manifest-list": file_uri(&list1),
                        "schema-id": 0
                    }
                ]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(metadata_dir.join("version-hint.text"), "1").unwrap();

        Self {
            _directory: directory,
            root,
            metadata,
            first_size,
            second_size,
        }
    }
}

fn data_entry(path: String, size: u64, rows: i64, content: i32) -> ManifestRow {
    ManifestRow {
        status: 1,
        data_file: ManifestDataFile {
            content,
            file_path: path,
            file_format: "PARQUET".into(),
            partition: EmptyPartition {},
            record_count: rows,
            file_size_bytes: i64::try_from(size).unwrap(),
        },
    }
}

fn delete_entry(path: String) -> ManifestRow {
    data_entry(path, 1, 1, 1)
}

fn write_manifest(path: &Path, rows: &[ManifestRow]) -> std::path::PathBuf {
    write_avro(path, MANIFEST_SCHEMA, rows)
}

fn write_manifest_list(path: &Path, manifests: &[(&Path, i32)]) -> std::path::PathBuf {
    let rows: Vec<_> = manifests
        .iter()
        .map(|(manifest, content)| ManifestListRow {
            manifest_path: file_uri(manifest),
            manifest_length: i64::try_from(fs::metadata(manifest).unwrap().len()).unwrap(),
            partition_spec_id: 0,
            content: *content,
        })
        .collect();
    write_avro(path, MANIFEST_LIST_SCHEMA, &rows)
}

fn write_avro<T: Serialize>(path: &Path, schema: &str, rows: &[T]) -> std::path::PathBuf {
    let schema = Schema::parse_str(schema).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    for row in rows {
        writer.append_ser(row).unwrap();
    }
    fs::write(path, writer.into_inner().unwrap()).unwrap();
    path.to_path_buf()
}

fn file_uri(path: &Path) -> String {
    Url::from_file_path(path).unwrap().into()
}

fn dir_uri(path: &Path) -> String {
    Url::from_directory_path(path).unwrap().into()
}

#[tokio::test]
async fn detect_names_iceberg_from_hint_metadata_json_and_table_root() {
    let fixture = Fixture::new();
    assert_eq!(
        table::detect(common::lake(), &fixture.root.to_string_lossy())
            .await
            .unwrap(),
        TableFormat::Iceberg
    );
    assert_eq!(
        table::detect(common::lake(), &fixture.metadata.to_string_lossy())
            .await
            .unwrap(),
        TableFormat::Iceberg
    );
}

#[tokio::test]
async fn load_resolves_uuid_named_metadata_without_a_hint() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata.parent().unwrap();
    let named = metadata.join("00001-22222222-2222-2222-2222-222222222222.metadata.json");
    fs::rename(&fixture.metadata, &named).unwrap();
    fs::remove_file(metadata.join("version-hint.text")).unwrap();
    let info = table::load(
        common::lake(),
        &load_request(fixture.root.to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert_eq!(info.snapshot_version, 1);
    assert!(!info.files.is_empty());
}

#[tokio::test]
async fn detect_prefers_delta_when_a_uniform_table_has_both_markers() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.root.join("_delta_log")).unwrap();
    assert_eq!(
        table::detect(common::lake(), &fixture.root.to_string_lossy())
            .await
            .unwrap(),
        TableFormat::Delta
    );
}

#[tokio::test]
async fn load_emits_active_files_and_names_delete_files_in_the_log() {
    let fixture = Fixture::new();
    let previous = table::load(
        common::lake(),
        &load_request(fixture.metadata.to_string_lossy(), Some(0)),
    )
    .await
    .unwrap();
    assert_eq!(previous.format, TableFormat::Iceberg);
    assert_eq!(previous.snapshot_version, 0);
    assert_eq!(previous.files.len(), 1);
    assert_eq!(previous.files[0].size_bytes, Some(fixture.first_size));
    assert_eq!(previous.log.len(), 1);

    let latest = table::load(
        common::lake(),
        &load_request(fixture.root.to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert_eq!(latest.snapshot_version, 1);
    assert_eq!(latest.files.len(), 2);
    let mut sizes: Vec<_> = latest
        .files
        .iter()
        .map(|file| file.size_bytes.unwrap_or(0))
        .collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [fixture.first_size, fixture.second_size]);
    let selected = latest
        .log
        .iter()
        .find(|commit| commit.version == 1)
        .unwrap();
    assert!(selected.actions.iter().any(|action| {
        action.kind.as_deref() == Some("delete")
            && action.path.as_deref() == Some("data/deletes.parquet")
    }));
    assert_eq!(latest.log.len(), 2);
}

#[tokio::test]
async fn file_list_walks_the_active_files() {
    let fixture = Fixture::new();
    let table = pqbench::v1::Table {
        name: "catalogs/local/schemas/local/tables/events".to_string(),
        uri: Some(fixture.metadata.to_string_lossy().into_owned()),
        ..pqbench::v1::Table::default()
    };
    let files: Vec<_> = pqbench::file::list::list(common::lake(), &table, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut paths: Vec<_> = files.iter().map(|file| file.path.as_str()).collect();
    paths.sort();
    assert_eq!(paths, ["data/first.parquet", "data/second.parquet"]);
    let first = files
        .iter()
        .find(|file| file.path == "data/first.parquet")
        .unwrap();
    assert_eq!(
        first.name,
        "catalogs/local/schemas/local/tables/events/files/data%2Ffirst.parquet"
    );

    let mut uris: Vec<_> = files.iter().map(|file| file.uri().unwrap()).collect();
    uris.sort();
    let mut expected = vec![
        fixture
            .root
            .join("data")
            .join("first.parquet")
            .to_string_lossy()
            .into_owned(),
        fixture
            .root
            .join("data")
            .join("second.parquet")
            .to_string_lossy()
            .into_owned(),
    ];
    expected.sort();
    assert_eq!(uris, expected);
    // Each file carries the size the manifest declares.
    let mut sizes: Vec<_> = files
        .iter()
        .map(|file| file.size_bytes.unwrap_or(0))
        .collect();
    sizes.sort_unstable();
    let mut expected_sizes = vec![fixture.first_size, fixture.second_size];
    expected_sizes.sort_unstable();
    assert_eq!(sizes, expected_sizes);
}

#[tokio::test]
async fn stream_files_resolves_relative_paths_against_the_table_location() {
    let fixture = Fixture::new();
    let metadata_dir = fixture.root.join("metadata");
    let manifest = write_manifest(
        &metadata_dir.join("relative.avro"),
        &[data_entry(
            "data/first.parquet".into(),
            fixture.first_size,
            3,
            0,
        )],
    );
    let list = write_manifest_list(&metadata_dir.join("snap-relative.avro"), &[(&manifest, 0)]);
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.metadata).unwrap()).unwrap();
    metadata["snapshots"][0]["manifest-list"] = serde_json::json!(file_uri(&list));
    fs::write(&fixture.metadata, serde_json::to_vec(&metadata).unwrap()).unwrap();

    let files: Vec<_> = table::stream_files(
        common::lake(),
        &load_request(fixture.metadata.to_string_lossy(), Some(0)),
    )
    .await
    .unwrap()
    .try_collect()
    .await
    .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "data/first.parquet");
    assert_eq!(
        files[0].uri().unwrap(),
        fixture
            .root
            .join("data/first.parquet")
            .to_string_lossy()
            .into_owned()
    );
}

#[tokio::test]
async fn load_then_bytemass_matches_footer_totals() {
    let fixture = Fixture::new();
    let info = table::load(
        common::lake(),
        &load_request(fixture.metadata.to_string_lossy(), None),
    )
    .await
    .unwrap();
    let mut measured_values = 0;
    let mut measured_bytes = 0;
    for file in &info.files {
        for column in common::columns(&file.uri().unwrap()).await {
            let mass = column.mass.as_ref().expect("a measured column");
            measured_values += mass.value_count.unwrap_or(0);
            measured_bytes += mass.compressed_bytes.unwrap_or(0);
        }
    }
    assert_eq!(measured_values, 8);
    let mut expected_bytes = 0;
    for relative in ["data/first.parquet", "data/second.parquet"] {
        let mass = default_metadata_parser()
            .read_masses(&fixture.root.join(relative))
            .unwrap();
        expected_bytes += mass.columns[0].compressed_bytes;
    }
    assert_eq!(measured_bytes, expected_bytes);
}

#[tokio::test]
async fn rejects_missing_snapshots_changed_files_and_path_escapes() {
    let fixture = Fixture::new();
    assert!(table::load(
        common::lake(),
        &load_request(fixture.metadata.to_string_lossy(), Some(99))
    )
    .await
    .is_err());

    let first = fixture.root.join("data/first.parquet");
    fs::write(&first, b"changed").unwrap();
    let info = table::load(
        common::lake(),
        &load_request(fixture.metadata.to_string_lossy(), Some(0)),
    )
    .await
    .unwrap();
    assert_eq!(info.files[0].size_bytes, Some(fixture.first_size));
    assert_ne!(fs::metadata(&first).unwrap().len(), fixture.first_size);

    write_parquet(&first, 3);
    let outside = tempfile::tempdir().unwrap();
    write_parquet(&outside.path().join("outside.parquet"), 2);
    let metadata_dir = fixture.root.join("metadata");
    let escaped = write_manifest(
        &metadata_dir.join("escape.avro"),
        &[data_entry(
            file_uri(&outside.path().join("outside.parquet")),
            fs::metadata(outside.path().join("outside.parquet"))
                .unwrap()
                .len(),
            2,
            0,
        )],
    );
    let list = write_manifest_list(&metadata_dir.join("snap-escape.avro"), &[(&escaped, 0)]);
    let metadata: serde_json::Value =
        serde_json::from_slice(&fs::read(&fixture.metadata).unwrap()).unwrap();
    let mut metadata = metadata;
    metadata["snapshots"][0]["manifest-list"] = serde_json::json!(file_uri(&list));
    fs::write(&fixture.metadata, serde_json::to_vec(&metadata).unwrap()).unwrap();
    let error = table::load(
        common::lake(),
        &load_request(fixture.metadata.to_string_lossy(), Some(0)),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(
        error.contains("outside the Iceberg table location"),
        "{error}"
    );
}
