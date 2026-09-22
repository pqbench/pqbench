#![cfg(feature = "iceberg")]

mod support;

use std::fs;
use std::path::Path;

use apache_avro::{Schema, Writer};
use pqbench::parquet_helpers::{default_metadata_parser, MetadataParser};
use pqbench::table::{self, LoadRequest, TableFormat};
use serde::Serialize;
use support::write_parquet;
use url::Url;

fn load_request(uri: impl Into<String>, version: Option<u64>) -> LoadRequest {
    LoadRequest {
        uri: uri.into(),
        version,
        env: Default::default(),
    }
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
    file_size_in_bytes: i64,
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
            file_size_in_bytes: i64::try_from(size).unwrap(),
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
        table::detect(&fixture.root.to_string_lossy(), &Default::default())
            .await
            .unwrap(),
        TableFormat::ICEBERG
    );
    assert_eq!(
        table::detect(&fixture.metadata.to_string_lossy(), &Default::default())
            .await
            .unwrap(),
        TableFormat::ICEBERG
    );
}

#[tokio::test]
async fn load_emits_active_files_and_names_delete_files_in_the_log() {
    let fixture = Fixture::new();
    let previous = table::load(&load_request(fixture.metadata.to_string_lossy(), Some(0)))
        .await
        .unwrap();
    assert_eq!(previous.kind, "pqbench.table");
    assert_eq!(previous.format, TableFormat::ICEBERG);
    assert_eq!(previous.snapshot_version, 0);
    assert_eq!(previous.files.len(), 1);
    assert_eq!(previous.files[0].size, fixture.first_size);
    assert_eq!(previous.log.len(), 2);

    let latest = table::load(&load_request(fixture.root.to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(latest.snapshot_version, 1);
    assert_eq!(latest.files.len(), 2);
    let mut sizes: Vec<_> = latest.files.iter().map(|file| file.size).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [fixture.first_size, fixture.second_size]);
    let selected = latest
        .log
        .iter()
        .find(|commit| commit.version == 1)
        .unwrap();
    assert!(selected
        .actions
        .iter()
        .any(|action| action.get("delete").is_some()));
    let text = table::render_text(&latest);
    assert!(text.contains("format: iceberg"));
    assert!(text.contains("delete"));
}

#[tokio::test]
async fn load_then_bytemass_matches_footer_totals() {
    let fixture = Fixture::new();
    let info = table::load(&load_request(fixture.metadata.to_string_lossy(), None))
        .await
        .unwrap();
    let rows = pqbench::bytemass::bytemass(&pqbench::bytemass::BytemassRequest {
        inputs: info.files.iter().map(|file| file.uri.clone()).collect(),
    })
    .await
    .unwrap();
    let summary = pqbench::bytemass::aggregate(&rows).unwrap();
    assert_eq!(summary.file_count, 2);
    assert_eq!(summary.num_rows, 8);
    let mut expected_bytes = 0;
    for relative in ["data/first.parquet", "data/second.parquet"] {
        let mass = default_metadata_parser()
            .read_masses(&fixture.root.join(relative))
            .unwrap();
        expected_bytes += mass.columns[0].bytes;
    }
    assert_eq!(summary.columns[0].compressed_bytes, expected_bytes);
}

#[tokio::test]
async fn rejects_missing_snapshots_changed_files_and_path_escapes() {
    let fixture = Fixture::new();
    assert!(
        table::load(&load_request(fixture.metadata.to_string_lossy(), Some(99)))
            .await
            .is_err()
    );

    let first = fixture.root.join("data/first.parquet");
    fs::write(&first, b"changed").unwrap();
    let error = table::load(&load_request(fixture.metadata.to_string_lossy(), Some(0)))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("size differs from manifest"), "{error}");

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
    let error = table::load(&load_request(fixture.metadata.to_string_lossy(), Some(0)))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("outside the Iceberg table location"),
        "{error}"
    );
}
