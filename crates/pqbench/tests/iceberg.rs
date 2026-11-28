#![cfg(feature = "iceberg")]

use crate::common;
use crate::support;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use apache_avro::{Schema, Writer};
use futures_util::TryStreamExt;
use pqbench::table::{self, LoadRequest};
use pqbench::third_party::parquet::read_file_masses;
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
        let mass = read_file_masses(&fixture.root.join(relative)).unwrap();
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

// ---------------------------------------------------------------------------
// Metadata-driven fixtures: partition values, delete files, snapshots.
//
// These tables name files the tests never read — `list partitions` and
// `list suggestions` decide from the metadata alone — so a fixture is a
// metadata JSON, a manifest list, and one manifest per snapshot.
// ---------------------------------------------------------------------------

use apache_avro::types::Value as Avro;
use pqbench::v1;

/// One manifest entry the fixture writes.
struct Entry {
    status: i32,
    snapshot_id: Option<i64>,
    content: i32,
    path: String,
    file_format: String,
    size_bytes: i64,
    record_count: i64,
    /// The `partition` record's values, in the manifest schema's field order.
    partition: Vec<(&'static str, Avro)>,
}

impl Entry {
    /// An added data file.
    fn data(
        path: &str,
        size_bytes: i64,
        record_count: i64,
        partition: &[(&'static str, Avro)],
    ) -> Self {
        Self {
            status: 1,
            snapshot_id: None,
            content: 0,
            path: path.into(),
            file_format: "PARQUET".into(),
            size_bytes,
            record_count,
            partition: partition.to_vec(),
        }
    }

    /// An entry with another status: `0` exists (rewritten into a new
    /// manifest), `2` was deleted.
    fn status(mut self, status: i32) -> Self {
        self.status = status;
        self
    }

    fn snapshot(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// A merge-on-read delete file: `1` position deletes, `2` equality deletes.
    fn delete(
        path: &str,
        size_bytes: i64,
        record_count: i64,
        content: i32,
        file_format: &str,
        partition: &[(&'static str, Avro)],
    ) -> Self {
        Self {
            status: 1,
            snapshot_id: None,
            content,
            path: path.into(),
            file_format: file_format.into(),
            size_bytes,
            record_count,
            partition: partition.to_vec(),
        }
    }
}

/// The manifest record, with the partition record's fields given by the spec.
fn manifest_schema(partition: &str) -> String {
    format!(
        r#"{{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {{"name": "status", "type": "int"}},
    {{"name": "snapshot_id", "type": ["null", "long"]}},
    {{"name": "data_file", "type": {{
      "type": "record",
      "name": "data_file",
      "fields": [
        {{"name": "content", "type": "int"}},
        {{"name": "file_path", "type": "string"}},
        {{"name": "file_format", "type": "string"}},
        {{"name": "partition", "type": {partition}}},
        {{"name": "record_count", "type": "long"}},
        {{"name": "file_size_in_bytes", "type": "long"}}
      ]
    }}}}
  ]
}}"#
    )
}

fn manifest_entry_value(entry: &Entry) -> Avro {
    Avro::Record(vec![
        ("status".into(), Avro::Int(entry.status)),
        (
            "snapshot_id".into(),
            Avro::Union(
                u32::from(entry.snapshot_id.is_some()),
                Box::new(entry.snapshot_id.map_or(Avro::Null, Avro::Long)),
            ),
        ),
        (
            "data_file".into(),
            Avro::Record(vec![
                ("content".into(), Avro::Int(entry.content)),
                ("file_path".into(), Avro::String(entry.path.clone())),
                (
                    "file_format".into(),
                    Avro::String(entry.file_format.clone()),
                ),
                (
                    "partition".into(),
                    Avro::Record(
                        entry
                            .partition
                            .iter()
                            .map(|(name, value)| ((*name).to_string(), value.clone()))
                            .collect(),
                    ),
                ),
                ("record_count".into(), Avro::Long(entry.record_count)),
                ("file_size_in_bytes".into(), Avro::Long(entry.size_bytes)),
            ]),
        ),
    ])
}

/// Write a manifest whose partition record has `partition_fields`, as
/// `(name, Avro type)` pairs.
fn write_manifest_entries(
    path: &Path,
    partition_fields: &[(&str, serde_json::Value)],
    entries: &[Entry],
) -> std::path::PathBuf {
    let fields: Vec<String> = partition_fields
        .iter()
        .map(|(name, avro_type)| format!(r#"{{"name":"{name}","type":{avro_type}}}"#))
        .collect();
    let partition = format!(
        r#"{{"type":"record","name":"r102","fields":[{}]}}"#,
        fields.join(",")
    );
    let schema = Schema::parse_str(&manifest_schema(&partition)).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    for entry in entries {
        writer.append(manifest_entry_value(entry)).unwrap();
    }
    fs::write(path, writer.into_inner().unwrap()).unwrap();
    path.to_path_buf()
}

/// A manifest-list row, with the fields a v2/v3 list carries.
#[derive(Serialize)]
struct ManifestListEntry {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32,
    added_snapshot_id: Option<i64>,
    deleted_files_count: Option<i32>,
}

const MANIFEST_LIST_SCHEMA_V3: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {"name": "manifest_path", "type": "string"},
    {"name": "manifest_length", "type": "long"},
    {"name": "partition_spec_id", "type": "int"},
    {"name": "content", "type": "int"},
    {"name": "added_snapshot_id", "type": ["null", "long"]},
    {"name": "deleted_files_count", "type": ["null", "int"]}
  ]
}"#;

/// One manifest a snapshot's list names.
struct ManifestRef<'a> {
    path: &'a Path,
    /// `0` data, `1` deletes.
    content: i32,
    partition_spec_id: i32,
    added_snapshot_id: Option<i64>,
    deleted_files_count: Option<i32>,
}

impl<'a> ManifestRef<'a> {
    /// The snapshot whose commit added the manifest; its entries then take
    /// their time from it unless they name their own.
    fn added(mut self, snapshot_id: i64) -> Self {
        self.added_snapshot_id = Some(snapshot_id);
        self
    }

    /// How many entries the manifest holds as deleted, which a v2+ list
    /// reports and a v1 list omits.
    fn deleted(mut self, count: i32) -> Self {
        self.deleted_files_count = Some(count);
        self
    }
}

fn data_manifest(path: &Path, partition_spec_id: i32) -> ManifestRef<'_> {
    ManifestRef {
        path,
        content: 0,
        partition_spec_id,
        added_snapshot_id: None,
        // A reader may skip reading a manifest that reports no deleted entry.
        deleted_files_count: Some(0),
    }
}

fn write_manifest_list_refs(path: &Path, manifests: &[ManifestRef<'_>]) -> std::path::PathBuf {
    let rows: Vec<ManifestListEntry> = manifests
        .iter()
        .map(|manifest| ManifestListEntry {
            manifest_path: file_uri(manifest.path),
            manifest_length: i64::try_from(fs::metadata(manifest.path).unwrap().len()).unwrap(),
            partition_spec_id: manifest.partition_spec_id,
            content: manifest.content,
            added_snapshot_id: manifest.added_snapshot_id,
            deleted_files_count: manifest.deleted_files_count,
        })
        .collect();
    write_avro(path, MANIFEST_LIST_SCHEMA_V3, &rows)
}

/// A snapshot the fixture metadata names.
struct SnapshotRef<'a> {
    id: i64,
    parent: Option<i64>,
    /// The commit's time, milliseconds since the Unix epoch.
    commit_time: i64,
    manifest_list: &'a Path,
}

fn snapshot_value(snapshot: &SnapshotRef<'_>) -> serde_json::Value {
    let mut value = serde_json::json!({
        "snapshot-id": snapshot.id,
        "sequence-number": snapshot.id,
        "timestamp-ms": snapshot.commit_time,
        "manifest-list": file_uri(snapshot.manifest_list),
        "schema-id": 0,
    });
    if let Some(parent) = snapshot.parent {
        value["parent-snapshot-id"] = serde_json::json!(parent);
    }
    value
}

/// A table's metadata document, written as `{name}` under `directory`.
struct Metadata {
    format_version: i32,
    location: String,
    /// The JSON value written under `schemas`.
    schema: serde_json::Value,
    /// The JSON value written under `partition-specs`.
    spec: serde_json::Value,
    default_spec_id: i32,
    current_snapshot_id: i64,
    /// The JSON value written under `snapshots`.
    snapshot: serde_json::Value,
}

impl Metadata {
    fn new(
        root: &Path,
        schemas: serde_json::Value,
        partition_specs: serde_json::Value,
        snapshots: serde_json::Value,
        current_snapshot_id: i64,
    ) -> Self {
        Self {
            format_version: 2,
            location: dir_uri(root),
            schema: schemas,
            spec: partition_specs,
            default_spec_id: 0,
            current_snapshot_id,
            snapshot: snapshots,
        }
    }

    fn write(&self, directory: &Path, name: &str) -> std::path::PathBuf {
        let path = directory.join(name);
        let document = serde_json::json!({
            "format-version": self.format_version,
            "table-uuid": "33333333-3333-3333-3333-333333333333",
            "location": self.location,
            "last-updated-ms": 0,
            "last-column-id": 100,
            "current-schema-id": 0,
            "schemas": self.schema,
            "default-spec-id": self.default_spec_id,
            "partition-specs": self.spec,
            "last-partition-id": 999,
            "current-snapshot-id": self.current_snapshot_id,
            "snapshots": self.snapshot,
        });
        fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        path
    }
}

/// The partitions the table at `metadata` lists.
async fn partitions(metadata: &Path, table: &str) -> Vec<v1::Partition> {
    let info = table::load(
        common::lake(),
        &load_request(metadata.to_string_lossy(), None),
    )
    .await
    .expect("a loadable table");
    let record = info.resource("local", "local", table);
    pqbench::partition::list::list(common::lake(), &record)
        .await
        .expect("a listed table")
}

fn suggestion(partition: &v1::Partition, rule: &str) -> Option<v1::Suggestion> {
    pqbench::lint::partition_suggestions(partition)
        .into_iter()
        .find(|suggestion| suggestion.rule == rule)
}

fn now_directory() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// A decimal's unscaled bytes, as the Avro decimal type carries them.
fn decimal_value(bytes: Vec<u8>) -> Avro {
    Avro::Decimal(apache_avro::Decimal::from(bytes))
}

fn uuid_bytes() -> Vec<u8> {
    vec![
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0x4d, 0xef, 0x8a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56,
        0x78,
    ]
}

/// The schema a partitioned fixture writes against: every type a transform or
/// an `identity` partition reads.
fn partitioned_schema() -> serde_json::Value {
    serde_json::json!([{
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "region", "required": false, "type": "string"},
            {"id": 3, "name": "day", "required": false, "type": "date"},
            {"id": 4, "name": "ts", "required": false, "type": "timestamptz"},
            {"id": 5, "name": "amount", "required": false, "type": "decimal(9, 2)"},
            {"id": 6, "name": "event_id", "required": false, "type": "uuid"},
            {"id": 7, "name": "name", "required": false, "type": "string"}
        ]
    }])
}

/// Every transform this build formats, one partition field each.
fn partitioned_spec() -> serde_json::Value {
    serde_json::json!([{
        "spec-id": 0,
        "fields": [
            {"source-id": 3, "field-id": 1000, "name": "day", "transform": "day"},
            {"source-id": 4, "field-id": 1001, "name": "hour", "transform": "hour"},
            {"source-id": 2, "field-id": 1002, "name": "region", "transform": "identity"},
            {"source-id": 5, "field-id": 1003, "name": "amount", "transform": "identity"},
            {"source-id": 6, "field-id": 1004, "name": "event_id", "transform": "identity"},
            {"source-id": 1, "field-id": 1005, "name": "id_bucket", "transform": "bucket[16]"},
            {"source-id": 7, "field-id": 1006, "name": "name_part", "transform": "truncate[2]"},
            {"source-id": 3, "field-id": 1007, "name": "year", "transform": "year"},
            {"source-id": 3, "field-id": 1008, "name": "month", "transform": "month"},
            {"source-id": 4, "field-id": 1009, "name": "ts", "transform": "identity"},
            {"source-id": 3, "field-id": 1010, "name": "day_identity", "transform": "identity"}
        ]
    }])
}

/// Each partition value in the manifest, in the spec's field order.
fn partitioned_partition(region: Avro) -> Vec<(&'static str, Avro)> {
    vec![
        ("day", Avro::Date(20_089)),
        ("hour", Avro::Int(481_321)),
        ("region", region),
        ("amount", decimal_value(vec![0x30, 0x39])),
        ("event_id", Avro::Fixed(16, uuid_bytes())),
        ("id_bucket", Avro::Int(7)),
        ("name_part", Avro::String("ab".into())),
        ("year", Avro::Int(55)),
        ("month", Avro::Int(660)),
        (
            "ts",
            Avro::Union(1, Box::new(Avro::TimestampMicros(1_735_689_600_000_000))),
        ),
        ("day_identity", Avro::Date(20_089)),
    ]
}

/// The Avro types of the partitioned manifest's `partition` record: a date as
/// its day count, a decimal as its unscaled bytes, a uuid as fixed bytes, and
/// the rest plain.
fn partitioned_fields() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        (
            "day",
            serde_json::json!({"type": "int", "logicalType": "date"}),
        ),
        ("hour", serde_json::json!("int")),
        ("region", serde_json::json!(["null", "string"])),
        (
            "amount",
            serde_json::json!({"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}),
        ),
        (
            "event_id",
            serde_json::json!({"type": "fixed", "name": "uuid_fixed", "size": 16}),
        ),
        ("id_bucket", serde_json::json!("int")),
        ("name_part", serde_json::json!("string")),
        ("year", serde_json::json!("int")),
        ("month", serde_json::json!("int")),
        (
            "ts",
            serde_json::json!(["null", {"type": "long", "logicalType": "timestamp-micros"}]),
        ),
        (
            "day_identity",
            serde_json::json!({"type": "int", "logicalType": "date"}),
        ),
    ]
}

/// A partitioned table's files become partitions whose values are the
/// partition record's values in the source column's terms, and whose totals
/// come from the manifests.
#[tokio::test]
async fn partitions_carry_each_transforms_value_and_the_manifests_totals() {
    let directory = now_directory();
    let root = directory.path();
    let metadata_dir = root.join("metadata");
    fs::create_dir_all(&metadata_dir).unwrap();

    let manifest = write_manifest_entries(
        &metadata_dir.join("m0.avro"),
        &partitioned_fields(),
        &[
            Entry::data(
                "data/a.parquet",
                1_000,
                10,
                &partitioned_partition(Avro::Union(1, Box::new(Avro::String("eu".into())))),
            ),
            Entry::data(
                "data/b.parquet",
                2_000,
                20,
                &partitioned_partition(Avro::Union(1, Box::new(Avro::String("eu".into())))),
            ),
            Entry::data(
                "data/c.parquet",
                4_000,
                40,
                &partitioned_partition(Avro::Union(0, Box::new(Avro::Null))),
            ),
        ],
    );
    let list = write_manifest_list_refs(
        &metadata_dir.join("snap.avro"),
        &[data_manifest(&manifest, 0).added(7)],
    );
    let metadata = Metadata::new(
        root,
        partitioned_schema(),
        partitioned_spec(),
        serde_json::json!([snapshot_value(&SnapshotRef {
            id: 7,
            parent: None,
            commit_time: 1_700_000_000_000,
            manifest_list: &list,
        })]),
        7,
    )
    .write(&metadata_dir, "v1.metadata.json");

    let records = partitions(&metadata, "events").await;
    assert_eq!(records.len(), 2);
    let eu = records
        .iter()
        .find(|partition| partition.partition_values["region"].as_deref() == Some("eu"))
        .expect("the eu partition");
    let null_region = records
        .iter()
        .find(|partition| partition.partition_values["region"].is_none())
        .expect("the null-region partition");

    let expected: BTreeMap<String, Option<String>> = [
        ("day", "2025-01-01"),
        ("hour", "2024-11-28-01"),
        ("region", "eu"),
        ("amount", "123.45"),
        ("event_id", "12345678-9abc-4def-8abc-def012345678"),
        ("id_bucket", "7"),
        ("name_part", "ab"),
        ("year", "2025"),
        ("month", "2025-01"),
        ("ts", "2025-01-01T00:00:00Z"),
        ("day_identity", "2025-01-01"),
    ]
    .into_iter()
    .map(|(field, value)| (field.to_string(), Some(value.to_string())))
    .collect();
    assert_eq!(eu.partition_values, expected);

    assert_eq!(eu.file_count, 2);
    assert_eq!(eu.size_bytes, 3_000);
    assert_eq!(eu.record_count, Some(30));
    assert_eq!(eu.first_file.as_ref().unwrap().path, "data/a.parquet");
    assert_eq!(eu.last_file.as_ref().unwrap().path, "data/b.parquet");
    assert_eq!(
        eu.first_file.as_ref().unwrap().modification_time,
        Some(1_700_000_000_000)
    );
    assert_eq!(eu.removed_file_count, Some(0));
    assert_eq!(eu.delete_file_count, None);

    // A null partition value is its own partition, and the partition id names
    // it the way Delta's does.
    assert_eq!(null_region.file_count, 1);
    assert_eq!(null_region.record_count, Some(40));
    let id = "day=2025-01-01/hour=2024-11-28-01/region=__HIVE_DEFAULT_PARTITION__/amount=123.45\
/event_id=12345678-9abc-4def-8abc-def012345678/id_bucket=7/name_part=ab/year=2025/month=2025-01\
/ts=2025-01-01T00:00:00Z/day_identity=2025-01-01";
    assert_eq!(
        null_region.name,
        v1::name::partition_name("catalogs/local/schemas/local/tables/events", id)
    );
}

/// Two specs: a file written under the old one has values the new spec's
/// fields do not mention, and a voided field keeps its old name. The partition
/// columns are the default spec's fields, then the older spec's.
#[tokio::test]
async fn a_partition_evolved_table_lists_every_specs_values() {
    let directory = now_directory();
    let root = directory.path();
    let metadata_dir = root.join("metadata");
    fs::create_dir_all(&metadata_dir).unwrap();

    let old = write_manifest_entries(
        &metadata_dir.join("m-old.avro"),
        &[("day", serde_json::json!("int"))],
        &[Entry::data(
            "data/old.parquet",
            1_000,
            10,
            &[("day", Avro::Int(20_089))],
        )],
    );
    let new = write_manifest_entries(
        &metadata_dir.join("m-new.avro"),
        &[
            ("region", serde_json::json!(["null", "string"])),
            // A void field's result type is `null`.
            ("old_day", serde_json::json!("null")),
        ],
        &[Entry::data(
            "data/new.parquet",
            2_000,
            20,
            &[
                (
                    "region",
                    Avro::Union(1, Box::new(Avro::String("eu".into()))),
                ),
                ("old_day", Avro::Null),
            ],
        )],
    );
    let list = write_manifest_list_refs(
        &metadata_dir.join("snap.avro"),
        &[
            data_manifest(&old, 0).added(7),
            data_manifest(&new, 1).added(7),
        ],
    );
    let metadata = Metadata {
        default_spec_id: 1,
        ..Metadata::new(
            root,
            partitioned_schema(),
            serde_json::json!([
                {
                    "spec-id": 0,
                    "fields": [
                        {"source-id": 3, "field-id": 1000, "name": "day", "transform": "day"}
                    ]
                },
                {
                    "spec-id": 1,
                    "fields": [
                        {"source-id": 2, "field-id": 1001, "name": "region", "transform": "identity"},
                        {"source-id": 3, "field-id": 1002, "name": "old_day", "transform": "void"}
                    ]
                }
            ]),
            serde_json::json!([snapshot_value(&SnapshotRef {
                id: 7,
                parent: None,
                commit_time: 1_700_000_000_000,
                manifest_list: &list,
            })]),
            7,
        )
    }
    .write(&metadata_dir, "v1.metadata.json");

    let info = table::load(
        common::lake(),
        &load_request(metadata.to_string_lossy(), None),
    )
    .await
    .expect("a loadable table");
    // The default spec's field first, then the older spec's; the voided field
    // is not a partition column at all.
    assert_eq!(info.partition_columns, ["region", "day"]);

    // Both specs' values are listed: neither `check_columns` nor the partition
    // name needs a column the table record does not carry.
    let records = partitions(&metadata, "events").await;
    assert_eq!(records.len(), 2);
    let day = records
        .iter()
        .find(|partition| partition.partition_values.contains_key("day"))
        .expect("the old spec's partition");
    assert_eq!(day.partition_values["day"].as_deref(), Some("2025-01-01"));
    assert_eq!(day.file_count, 1);
    let region = records
        .iter()
        .find(|partition| partition.partition_values.contains_key("region"))
        .expect("the new spec's partition");
    assert_eq!(region.partition_values["region"].as_deref(), Some("eu"));
    assert!(!region.partition_values.contains_key("old_day"));
    assert_eq!(region.file_count, 1);
}

/// A v3 table's position deletes, equality deletes, and Puffin deletion vector
/// are delete files: counted per partition, mixed into the etag, and worth a
/// rewrite.
#[tokio::test]
async fn delete_files_are_counted_remedied_and_change_the_etag() {
    let directory = now_directory();
    let root = directory.path();
    let metadata_dir = root.join("metadata");
    fs::create_dir_all(&metadata_dir).unwrap();

    let partition = |region: &'static str| {
        vec![(
            "region",
            Avro::Union(1, Box::new(Avro::String(region.into()))).clone(),
        )]
    };
    let fields = [("region", serde_json::json!(["null", "string"]))];
    let data = write_manifest_entries(
        &metadata_dir.join("data.avro"),
        &fields,
        &[Entry::data(
            "data/a.parquet",
            4_000_000,
            40,
            &partition("eu"),
        )],
    );
    let position = write_manifest_entries(
        &metadata_dir.join("position.avro"),
        &fields,
        &[
            Entry::delete(
                "data/a-deletes.parquet",
                1_500_000,
                15,
                1,
                "PARQUET",
                &partition("eu"),
            ),
            Entry::delete(
                "data/a-vector.puffin",
                400_000,
                5,
                1,
                "PUFFIN",
                &partition("eu"),
            ),
        ],
    );
    let equality = write_manifest_entries(
        &metadata_dir.join("equality.avro"),
        &fields,
        &[Entry::delete(
            "data/a-equality.avro",
            200_000,
            2,
            2,
            "AVRO",
            &partition("eu"),
        )],
    );
    let all = write_manifest_list_refs(
        &metadata_dir.join("all.avro"),
        &[
            data_manifest(&data, 0).added(7),
            ManifestRef {
                content: 1,
                ..data_manifest(&position, 0).added(7)
            },
            ManifestRef {
                content: 1,
                ..data_manifest(&equality, 0).added(7)
            },
        ],
    );
    // The same table after `rewrite_position_delete_files`: the Puffin vector
    // is rewritten into the position-delete file, so one delete file fewer.
    let fewer = write_manifest_list_refs(
        &metadata_dir.join("fewer.avro"),
        &[
            data_manifest(&data, 0).added(7),
            ManifestRef {
                content: 1,
                ..data_manifest(&position, 0).added(7)
            },
        ],
    );
    let schemas = serde_json::json!([{
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "region", "required": false, "type": "string"}
        ]
    }]);
    let spec = serde_json::json!([{
        "spec-id": 0,
        "fields": [
            {"source-id": 2, "field-id": 1000, "name": "region", "transform": "identity"}
        ]
    }]);
    let snapshot = |list: &Path| {
        serde_json::json!([snapshot_value(&SnapshotRef {
            id: 7,
            parent: None,
            commit_time: 1_700_000_000_000,
            manifest_list: list,
        })])
    };
    let version = |list: &Path, name: &str| {
        Metadata {
            format_version: 3,
            ..Metadata::new(root, schemas.clone(), spec.clone(), snapshot(list), 7)
        }
        .write(&metadata_dir, name)
    };
    let with_all = version(&all, "all.metadata.json");
    let with_fewer = version(&fewer, "fewer.metadata.json");

    let records = partitions(&with_all, "events").await;
    assert_eq!(records.len(), 1);
    let partition = &records[0];
    assert_eq!(partition.file_count, 1);
    assert_eq!(partition.size_bytes, 4_000_000);
    assert_eq!(partition.delete_file_count, Some(3));
    assert_eq!(partition.delete_size_bytes, Some(2_100_000));

    let suggestion =
        suggestion(partition, pqbench::lint::DELETE_FILES_RULE).expect("a delete-files suggestion");
    assert!(
        suggestion.message.contains("3 delete files"),
        "{}",
        suggestion.message
    );
    assert!(
        suggestion.message.contains("2.0 MiB"),
        "{}",
        suggestion.message
    );
    assert_eq!(
        suggestion.remedy,
        "ALTER TABLE <table> EXECUTE rewrite_data_files; \
         ALTER TABLE <table> EXECUTE rewrite_position_delete_files"
    );
    for engine in ["iceberg", "spark"] {
        assert!(
            suggestion
                .remedies
                .iter()
                .any(|remedy| remedy.engine == engine),
            "{engine} has no remedy"
        );
    }

    // One delete file fewer is a different partition: a cached record is
    // invalidated when the deletes are rewritten.
    let rewritten = partitions(&with_fewer, "events").await;
    assert_eq!(rewritten[0].delete_file_count, Some(2));
    assert_eq!(rewritten[0].delete_size_bytes, Some(1_900_000));
    assert_ne!(rewritten[0].etag, partition.etag);
    assert_eq!(rewritten[0].file_count, partition.file_count);
}

/// A snapshot that superseded a file, and an entry it marks deleted, are the
/// files still billed; a file the two snapshots share is live and not counted.
#[tokio::test]
async fn superseded_and_deleted_entries_are_removed_files() {
    let directory = now_directory();
    let root = directory.path();
    let metadata_dir = root.join("metadata");
    fs::create_dir_all(&metadata_dir).unwrap();

    let fields = [("day", serde_json::json!("int"))];
    let day = || vec![("day", Avro::Int(20_089))];
    let first = write_manifest_entries(
        &metadata_dir.join("m0.avro"),
        &fields,
        &[
            Entry::data("data/shared.parquet", 1_000, 10, &day()).snapshot(1),
            Entry::data("data/superseded.parquet", 2 * 1024 * 1024, 20, &day()).snapshot(1),
        ],
    );
    let second = write_manifest_entries(
        &metadata_dir.join("m1.avro"),
        &fields,
        &[
            Entry::data("data/shared.parquet", 1_000, 10, &day())
                .status(0)
                .snapshot(1),
            Entry::data("data/superseded.parquet", 2 * 1024 * 1024, 20, &day())
                .status(2)
                .snapshot(2),
            Entry::data("data/current.parquet", 500, 5, &day()).snapshot(2),
        ],
    );
    let older = write_manifest_list_refs(
        &metadata_dir.join("snap-1.avro"),
        &[ManifestRef {
            deleted_files_count: None,
            ..data_manifest(&first, 0).added(1)
        }],
    );
    let current = write_manifest_list_refs(
        &metadata_dir.join("snap-2.avro"),
        &[data_manifest(&second, 0).added(2).deleted(1)],
    );
    let metadata = Metadata::new(
        root,
        serde_json::json!([{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "day", "required": false, "type": "date"}
            ]
        }]),
        serde_json::json!([{
            "spec-id": 0,
            "fields": [
                {"source-id": 2, "field-id": 1000, "name": "day", "transform": "day"}
            ]
        }]),
        serde_json::json!([
            snapshot_value(&SnapshotRef {
                id: 1,
                parent: None,
                commit_time: 1_600_000_000_000,
                manifest_list: &older,
            }),
            snapshot_value(&SnapshotRef {
                id: 2,
                parent: Some(1),
                commit_time: 1_700_000_000_000,
                manifest_list: &current,
            }),
        ]),
        2,
    )
    .write(&metadata_dir, "v1.metadata.json");

    let records = partitions(&metadata, "events").await;
    assert_eq!(records.len(), 1);
    let partition = &records[0];
    // The shared file and the current one are live.
    assert_eq!(partition.file_count, 2);
    assert_eq!(partition.size_bytes, 1_500);
    assert_eq!(partition.record_count, Some(15));
    assert_eq!(
        partition.last_file.as_ref().unwrap().modification_time,
        Some(1_700_000_000_000)
    );
    // The superseded file is billed once, though two snapshots name it.
    assert_eq!(partition.removed_file_count, Some(1));
    assert_eq!(partition.removed_size_bytes, Some(2 * 1024 * 1024));

    let suggestion =
        suggestion(partition, pqbench::lint::TOMBSTONES_RULE).expect("a tombstones suggestion");
    assert_eq!(suggestion.verified, Some(true));
    assert_eq!(suggestion.savings_bytes, Some(2 * 1024 * 1024));
}

/// A table with one snapshot and no deleted entry still reports its removed
/// files: zero, as Delta does, so a caller's record has the same shape.
#[tokio::test]
async fn a_single_snapshot_table_reports_no_removed_files() {
    let directory = now_directory();
    let root = directory.path();
    let metadata_dir = root.join("metadata");
    fs::create_dir_all(&metadata_dir).unwrap();

    let manifest = write_manifest_entries(
        &metadata_dir.join("m0.avro"),
        &[("day", serde_json::json!("int"))],
        &[Entry::data(
            "data/a.parquet",
            1_000,
            10,
            &[("day", Avro::Int(20_089))],
        )],
    );
    let list = write_manifest_list_refs(
        &metadata_dir.join("snap.avro"),
        &[data_manifest(&manifest, 0).added(7)],
    );
    let metadata = Metadata::new(
        root,
        serde_json::json!([{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "day", "required": false, "type": "date"}
            ]
        }]),
        serde_json::json!([{
            "spec-id": 0,
            "fields": [
                {"source-id": 2, "field-id": 1000, "name": "day", "transform": "day"}
            ]
        }]),
        serde_json::json!([snapshot_value(&SnapshotRef {
            id: 7,
            parent: None,
            commit_time: 1_700_000_000_000,
            manifest_list: &list,
        })]),
        7,
    )
    .write(&metadata_dir, "v1.metadata.json");

    let records = partitions(&metadata, "events").await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].removed_file_count, Some(0));
    assert_eq!(records[0].removed_size_bytes, Some(0));
    assert_eq!(records[0].delete_file_count, None);
    assert!(suggestion(&records[0], pqbench::lint::TOMBSTONES_RULE).is_none());
}

/// Format versions 1, 2, and 3 share one document and one manifest format; a
/// version this build does not know is refused, and nothing else is.
#[tokio::test]
async fn every_known_format_version_loads_and_no_other_does() {
    let directory = now_directory();
    let root = directory.path();
    let metadata_dir = root.join("metadata");
    fs::create_dir_all(&metadata_dir).unwrap();

    let manifest = write_manifest_entries(
        &metadata_dir.join("m0.avro"),
        &[],
        &[Entry::data("data/a.parquet", 1_000, 10, &[])],
    );
    let list = write_manifest_list_refs(
        &metadata_dir.join("snap.avro"),
        &[data_manifest(&manifest, 0).added(7)],
    );
    let document = |format_version: i32| {
        Metadata {
            format_version,
            ..Metadata::new(
                root,
                serde_json::json!([{
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
                }]),
                serde_json::json!([{"spec-id": 0, "fields": []}]),
                serde_json::json!([snapshot_value(&SnapshotRef {
                    id: 7,
                    parent: None,
                    commit_time: 1_700_000_000_000,
                    manifest_list: &list,
                })]),
                7,
            )
        }
        .write(&metadata_dir, &format!("v{format_version}.metadata.json"))
    };

    for format_version in [1, 2, 3] {
        let metadata = document(format_version);
        let info = table::load(
            common::lake(),
            &load_request(metadata.to_string_lossy(), None),
        )
        .await
        .unwrap_or_else(|error| panic!("format version {format_version}: {error}"));
        assert_eq!(info.files.len(), 1);
    }
    for format_version in [0, 4] {
        let metadata = document(format_version);
        let error = table::load(
            common::lake(),
            &load_request(metadata.to_string_lossy(), None),
        )
        .await
        .expect_err("an unknown format version");
        assert!(
            error
                .to_string()
                .contains("unsupported Iceberg format version"),
            "{error}"
        );
    }
}

/// The value reader keeps each Avro value's own type, which is what lets a
/// reader interpret a manifest's partition record without a Rust type per
/// partition spec.
#[test]
fn avro_values_keep_their_types() {
    use pqbench::third_party::avro::Value as Avro2;

    const SCHEMA: &str = r#"{
      "type": "record",
      "name": "entry",
      "fields": [
        {"name": "nothing", "type": "null"},
        {"name": "flag", "type": "boolean"},
        {"name": "small", "type": "int"},
        {"name": "big", "type": "long"},
        {"name": "text", "type": "string"},
        {"name": "blob", "type": "bytes"},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "moment", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "local", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
        {"name": "maybe", "type": ["null", "string"]},
        {"name": "items", "type": {"type": "array", "items": "int"}},
        {"name": "tags", "type": {"type": "map", "values": "int"}},
        {"name": "body", "type": {"type": "record", "name": "inner", "fields": [
          {"name": "leaf", "type": "int"}
        ]}}
      ]
    }"#;
    let row = Avro::Record(vec![
        ("nothing".into(), Avro::Null),
        ("flag".into(), Avro::Boolean(true)),
        ("small".into(), Avro::Int(-3)),
        ("big".into(), Avro::Long(1 << 40)),
        ("text".into(), Avro::String("eu".into())),
        ("blob".into(), Avro::Bytes(vec![0x0f])),
        ("day".into(), Avro::Date(20_089)),
        (
            "moment".into(),
            Avro::TimestampMicros(1_735_689_600_000_000),
        ),
        (
            "local".into(),
            Avro::LocalTimestampMicros(1_735_689_600_000_000),
        ),
        ("amount".into(), decimal_value(vec![0x30, 0x39])),
        (
            "maybe".into(),
            Avro::Union(1, Box::new(Avro::String("yes".into()))),
        ),
        (
            "items".into(),
            Avro::Array(vec![Avro::Int(1), Avro::Int(2)]),
        ),
        (
            "tags".into(),
            Avro::Map(std::collections::HashMap::from([(
                "a".to_string(),
                Avro::Int(1),
            )])),
        ),
        (
            "body".into(),
            Avro::Record(vec![("leaf".into(), Avro::Int(7))]),
        ),
    ]);
    let directory = now_directory();
    let path = directory.path().join("value.avro");
    let schema = Schema::parse_str(SCHEMA).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    writer.append(row).unwrap();
    fs::write(&path, writer.into_inner().unwrap()).unwrap();

    let values = pqbench::third_party::avro::read_avro_values(&fs::read(&path).unwrap(), "value")
        .expect("a readable container");
    let fields = match &values[..] {
        [Avro2::Record(fields)] => fields,
        other => panic!("one record expected, got {other:?}"),
    };
    let field = |name: &str| {
        fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.clone())
            .expect("a field")
    };
    // The union's index is the writer's detail; the value is what a reader uses.
    assert_eq!(field("nothing"), Avro2::Null);
    assert_eq!(field("flag"), Avro2::Bool(true));
    assert_eq!(field("small"), Avro2::Int(-3));
    assert_eq!(field("big"), Avro2::Long(1 << 40));
    assert_eq!(field("text"), Avro2::String("eu".into()));
    assert_eq!(field("blob"), Avro2::Bytes(vec![0x0f]));
    assert_eq!(field("day"), Avro2::Date(20_089));
    assert_eq!(
        field("moment"),
        Avro2::TimestampMicros(1_735_689_600_000_000)
    );
    assert_eq!(
        field("local"),
        Avro2::TimestampMicros(1_735_689_600_000_000)
    );
    assert_eq!(field("amount"), Avro2::Decimal(vec![0x30, 0x39]));
    assert_eq!(field("maybe"), Avro2::String("yes".into()));
    assert_eq!(
        field("items"),
        Avro2::Array(vec![Avro2::Int(1), Avro2::Int(2)])
    );
    assert_eq!(
        field("tags"),
        Avro2::Map(BTreeMap::from([("a".to_string(), Avro2::Int(1))]))
    );
    assert_eq!(
        field("body"),
        Avro2::Record(vec![("leaf".into(), Avro2::Int(7))])
    );
}

/// A file's time is the commit time of the snapshot that added it, so a window
/// over a table selects the commits it names, as it does for Delta.
#[tokio::test]
async fn a_window_selects_the_files_of_the_snapshots_it_names() {
    let directory = now_directory();
    let root = directory.path();
    let metadata_dir = root.join("metadata");
    fs::create_dir_all(&metadata_dir).unwrap();

    let day = || vec![("day", Avro::Int(20_089))];
    let fields = [("day", serde_json::json!("int"))];
    let first = write_manifest_entries(
        &metadata_dir.join("m0.avro"),
        &fields,
        &[Entry::data("data/old.parquet", 1_000, 10, &day()).snapshot(1)],
    );
    let second = write_manifest_entries(
        &metadata_dir.join("m1.avro"),
        &fields,
        &[Entry::data("data/new.parquet", 2_000, 20, &day()).snapshot(2)],
    );
    let older = write_manifest_list_refs(
        &metadata_dir.join("snap-1.avro"),
        &[data_manifest(&first, 0).added(1)],
    );
    let newer = write_manifest_list_refs(
        &metadata_dir.join("snap-2.avro"),
        &[data_manifest(&second, 0).added(2)],
    );
    let metadata = Metadata::new(
        root,
        serde_json::json!([{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "day", "required": false, "type": "date"}
            ]
        }]),
        serde_json::json!([{
            "spec-id": 0,
            "fields": [
                {"source-id": 2, "field-id": 1000, "name": "day", "transform": "day"}
            ]
        }]),
        serde_json::json!([
            snapshot_value(&SnapshotRef {
                id: 1,
                parent: None,
                commit_time: 1_600_000_000_000,
                manifest_list: &older,
            }),
            snapshot_value(&SnapshotRef {
                id: 2,
                parent: Some(1),
                commit_time: 1_700_000_000_000,
                manifest_list: &newer,
            }),
        ]),
        2,
    )
    .write(&metadata_dir, "v1.metadata.json");

    let info = table::load(
        common::lake(),
        &load_request(metadata.to_string_lossy(), None),
    )
    .await
    .expect("a loadable table");
    let record = info.resource("local", "local", "events");
    let window = pqbench::time::TimeWindow::day(1_700_000_000_000);
    let files: Vec<_> = pqbench::file::list::list(common::lake(), &record, Some(window))
        .await
        .expect("a listed table")
        .try_collect()
        .await
        .expect("a window over Iceberg");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "data/new.parquet");
    assert_eq!(files[0].modification_time, Some(1_700_000_000_000));
}

/// Iceberg v2 and v3 manifest lists declare `added_snapshot_id` (`long`) and
/// `deleted_files_count` (`int`) as required fields rather than `["null", T]`
/// unions.
#[tokio::test(flavor = "current_thread")]
async fn v2_manifest_list_with_required_integers_loads() {
    #[derive(Serialize)]
    struct RequiredManifestListRow {
        manifest_path: String,
        manifest_length: i64,
        partition_spec_id: i32,
        content: i32,
        added_snapshot_id: i64,
        deleted_files_count: i32,
    }

    const REQUIRED_MANIFEST_LIST_SCHEMA: &str = r#"{
      "type": "record",
      "name": "manifest_file",
      "fields": [
        {"name": "manifest_path", "type": "string"},
        {"name": "manifest_length", "type": "long"},
        {"name": "partition_spec_id", "type": "int"},
        {"name": "content", "type": "int"},
        {"name": "added_snapshot_id", "type": "long"},
        {"name": "deleted_files_count", "type": "int"}
      ]
    }"#;

    let directory = now_directory();
    let root = directory.path();
    let data_dir = root.join("data");
    let metadata_dir = root.join("metadata");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&metadata_dir).unwrap();

    let parquet = data_dir.join("part-0.parquet");
    write_parquet(&parquet, 4);
    let size = fs::metadata(&parquet).unwrap().len();
    let manifest = write_manifest(
        &metadata_dir.join("m0.avro"),
        &[data_entry(file_uri(&parquet), size, 4, 0)],
    );
    let manifest_list = write_avro(
        &metadata_dir.join("snap-1.avro"),
        REQUIRED_MANIFEST_LIST_SCHEMA,
        &[RequiredManifestListRow {
            manifest_path: file_uri(&manifest),
            manifest_length: i64::try_from(fs::metadata(&manifest).unwrap().len()).unwrap(),
            partition_spec_id: 0,
            content: 0,
            added_snapshot_id: 1,
            deleted_files_count: 0,
        }],
    );
    let metadata = Metadata::new(
        root,
        serde_json::json!([{
            "schema-id": 0,
            "fields": [{"id": 1, "name": "id", "type": "long", "required": false}]
        }]),
        serde_json::json!([{"spec-id": 0, "fields": []}]),
        serde_json::json!([snapshot_value(&SnapshotRef {
            id: 1,
            parent: None,
            commit_time: 1_710_000_000_000,
            manifest_list: &manifest_list,
        })]),
        1,
    )
    .write(&metadata_dir, "v1.metadata.json");

    let listed = partitions(&metadata, "events").await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].file_count, 1);
    assert_eq!(listed[0].record_count, Some(4));
    assert_eq!(
        listed[0].first_file.as_ref().unwrap().modification_time,
        Some(1_710_000_000_000)
    );
}
