#![cfg(feature = "iceberg")]

use crate::common;
use crate::support;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use apache_avro::types::Value as Avro;
use apache_avro::{Schema, Writer};
use futures_util::TryStreamExt;
use pqbench::table::{self, LoadRequest};
use pqbench::third_party::avro::Value as Avro2;
use pqbench::third_party::parquet::read_file_masses;
use pqbench::v1::{self, TableFormat};
use serde::Serialize;
use serde_json::json;
use support::write_parquet;
use url::Url;

fn load_request(uri: impl Into<String>, version: Option<u64>) -> LoadRequest {
    LoadRequest::new(uri, version)
}

fn file_uri(path: &Path) -> String {
    Url::from_file_path(path).unwrap().into()
}

fn dir_uri(path: &Path) -> String {
    Url::from_directory_path(path).unwrap().into()
}

fn write_avro<T: Serialize>(path: &Path, schema: &str, rows: &[T]) -> PathBuf {
    let schema = Schema::parse_str(schema).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    for row in rows {
        writer.append_ser(row).unwrap();
    }
    fs::write(path, writer.into_inner().unwrap()).unwrap();
    path.to_path_buf()
}

struct Entry {
    status: i32,
    snapshot_id: Option<i64>,
    content: i32,
    path: String,
    file_format: String,
    size_bytes: i64,
    record_count: i64,
    partition: Vec<(&'static str, Avro)>,
}

impl Entry {
    fn data(
        path: impl Into<String>,
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

    fn delete(
        path: impl Into<String>,
        size_bytes: i64,
        record_count: i64,
        content: i32,
        file_format: &str,
        partition: &[(&'static str, Avro)],
    ) -> Self {
        Self {
            content,
            file_format: file_format.into(),
            ..Self::data(path, size_bytes, record_count, partition)
        }
    }

    fn status(mut self, status: i32) -> Self {
        self.status = status;
        self
    }

    fn snapshot(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }
}

#[derive(Serialize)]
struct ManifestListRow {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32,
    added_snapshot_id: Option<i64>,
    deleted_files_count: Option<i32>,
}

const MANIFEST_LIST_SCHEMA: &str = r#"{
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

fn write_entries(
    path: &Path,
    partition_fields: &[(&str, serde_json::Value)],
    entries: &[Entry],
) -> PathBuf {
    let fields: Vec<String> = partition_fields
        .iter()
        .map(|(name, ty)| format!(r#"{{"name":"{name}","type":{ty}}}"#))
        .collect();
    let schema = Schema::parse_str(&format!(
        r#"{{
          "type": "record", "name": "manifest_entry", "fields": [
            {{"name": "status", "type": "int"}},
            {{"name": "snapshot_id", "type": ["null", "long"]}},
            {{"name": "data_file", "type": {{
              "type": "record", "name": "data_file", "fields": [
                {{"name": "content", "type": "int"}},
                {{"name": "file_path", "type": "string"}},
                {{"name": "file_format", "type": "string"}},
                {{"name": "partition", "type": {{"type":"record","name":"r102","fields":[{}]}}}},
                {{"name": "record_count", "type": "long"}},
                {{"name": "file_size_in_bytes", "type": "long"}}
              ]
            }}}}
          ]
        }}"#,
        fields.join(",")
    ))
    .unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    for entry in entries {
        writer
            .append(Avro::Record(vec![
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
                                    .map(|(k, v)| ((*k).into(), v.clone()))
                                    .collect(),
                            ),
                        ),
                        ("record_count".into(), Avro::Long(entry.record_count)),
                        ("file_size_in_bytes".into(), Avro::Long(entry.size_bytes)),
                    ]),
                ),
            ]))
            .unwrap();
    }
    fs::write(path, writer.into_inner().unwrap()).unwrap();
    path.to_path_buf()
}

fn write_list(path: &Path, manifests: &[(&Path, i32, i32, Option<i64>)]) -> PathBuf {
    let rows: Vec<_> = manifests
        .iter()
        .map(|(manifest, content, spec_id, added)| ManifestListRow {
            manifest_path: file_uri(manifest),
            manifest_length: fs::metadata(manifest).unwrap().len() as i64,
            partition_spec_id: *spec_id,
            content: *content,
            added_snapshot_id: *added,
            deleted_files_count: None,
        })
        .collect();
    write_avro(path, MANIFEST_LIST_SCHEMA, &rows)
}

struct TestTable {
    // aipnaming: allow(aip-140/underscores)
    _dir: tempfile::TempDir,
    root: PathBuf,
    meta_dir: PathBuf,
    format_version: i32,
    default_spec_id: i32,
}

impl TestTable {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let meta_dir = root.join("metadata");
        fs::create_dir_all(root.join("data")).unwrap();
        fs::create_dir_all(&meta_dir).unwrap();
        Self {
            _dir: dir,
            root,
            meta_dir,
            format_version: 2,
            default_spec_id: 0,
        }
    }

    fn manifest(
        &self,
        name: &str,
        fields: &[(&str, serde_json::Value)],
        entries: &[Entry],
    ) -> PathBuf {
        write_entries(&self.meta_dir.join(name), fields, entries)
    }

    fn list(&self, name: &str, manifests: &[(&Path, i32, i32, Option<i64>)]) -> PathBuf {
        write_list(&self.meta_dir.join(name), manifests)
    }

    fn write_metadata(
        &self,
        name: &str,
        fields: serde_json::Value,
        specs: serde_json::Value,
        snapshots: &[(i64, Option<i64>, i64, &Path)],
    ) -> PathBuf {
        let current = snapshots.last().map_or(-1, |s| s.0);
        let snaps: Vec<_> = snapshots
            .iter()
            .map(|(id, parent, ts, list)| {
                let mut s = json!({
                    "snapshot-id": id,
                    "sequence-number": id,
                    "timestamp-ms": ts,
                    "manifest-list": file_uri(list),
                    "schema-id": 0,
                });
                if let Some(p) = parent {
                    s["parent-snapshot-id"] = json!(p);
                }
                s
            })
            .collect();
        let path = self.meta_dir.join(name);
        fs::write(
            &path,
            serde_json::to_vec_pretty(&json!({
                "format-version": self.format_version,
                "table-uuid": "11111111-1111-1111-1111-111111111111",
                "location": dir_uri(&self.root),
                "last-updated-ms": 0,
                "last-column-id": 100,
                "current-schema-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": fields}],
                "default-spec-id": self.default_spec_id,
                "partition-specs": specs,
                "last-partition-id": 999,
                "current-snapshot-id": current,
                "snapshots": snaps,
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }
}

struct Fixture {
    table: TestTable,
    metadata: PathBuf,
    first_size: u64,
    second_size: u64,
}

impl Fixture {
    fn new() -> Self {
        let table = TestTable::new();
        let first = table.root.join("data/first.parquet");
        let second = table.root.join("data/second.parquet");
        write_parquet(&first, 3);
        write_parquet(&second, 5);
        let first_size = fs::metadata(&first).unwrap().len();
        let second_size = fs::metadata(&second).unwrap().len();

        let m0 = table.manifest(
            "m0.avro",
            &[],
            &[Entry::data(file_uri(&first), first_size as i64, 3, &[])],
        );
        let m1 = table.manifest(
            "m1.avro",
            &[],
            &[
                Entry::data(file_uri(&first), first_size as i64, 3, &[]),
                Entry::data(file_uri(&second), second_size as i64, 5, &[]),
            ],
        );
        let d1 = table.manifest(
            "d1.avro",
            &[],
            &[Entry::delete(
                file_uri(&table.root.join("data/deletes.parquet")),
                1,
                1,
                1,
                "PARQUET",
                &[],
            )],
        );
        let s0 = table.list("snap-0.avro", &[(&m0, 0, 0, Some(0))]);
        let s1 = table.list("snap-1.avro", &[(&m1, 0, 0, Some(1)), (&d1, 1, 0, Some(1))]);
        let metadata = table.write_metadata(
            "v1.metadata.json",
            json!([{"id": 1, "name": "id", "required": true, "type": "long"}]),
            json!([{"spec-id": 0, "fields": []}]),
            &[(0, None, 0, &s0), (1, Some(0), 1, &s1)],
        );
        fs::write(table.meta_dir.join("version-hint.text"), "1").unwrap();
        Self {
            table,
            metadata,
            first_size,
            second_size,
        }
    }
}

async fn partitions(metadata: &Path, table: &str) -> Vec<v1::Partition> {
    let info = table::load(
        common::lake(),
        &load_request(metadata.to_string_lossy(), None),
    )
    .await
    .unwrap();
    pqbench::partition::list::list(common::lake(), &info.resource("local", "local", table))
        .await
        .unwrap()
}

fn suggestion(partition: &v1::Partition, rule: &str) -> Option<v1::Suggestion> {
    pqbench::lint::partition_suggestions(partition)
        .into_iter()
        .find(|item| item.rule == rule)
}

#[tokio::test]
async fn detect_names_iceberg_from_hint_metadata_json_and_table_root() {
    let f = Fixture::new();
    for path in [&f.table.root, &f.metadata] {
        assert_eq!(
            table::detect(common::lake(), &path.to_string_lossy())
                .await
                .unwrap(),
            TableFormat::Iceberg
        );
    }
}

#[tokio::test]
async fn load_resolves_uuid_named_metadata_without_a_hint() {
    let f = Fixture::new();
    let named = f
        .table
        .meta_dir
        .join("00001-22222222-2222-2222-2222-222222222222.metadata.json");
    fs::rename(&f.metadata, &named).unwrap();
    fs::remove_file(f.table.meta_dir.join("version-hint.text")).unwrap();
    let info = table::load(
        common::lake(),
        &load_request(f.table.root.to_string_lossy(), None),
    )
    .await
    .unwrap();
    assert_eq!(info.snapshot_version, 1);
    assert!(!info.files.is_empty());
}

#[tokio::test]
async fn detect_prefers_delta_when_a_uniform_table_has_both_markers() {
    let f = Fixture::new();
    fs::create_dir(f.table.root.join("_delta_log")).unwrap();
    assert_eq!(
        table::detect(common::lake(), &f.table.root.to_string_lossy())
            .await
            .unwrap(),
        TableFormat::Delta
    );
}

#[tokio::test]
async fn load_emits_active_files_and_names_delete_files_in_the_log() {
    let f = Fixture::new();
    let previous = table::load(
        common::lake(),
        &load_request(f.metadata.to_string_lossy(), Some(0)),
    )
    .await
    .unwrap();
    assert_eq!(previous.format, TableFormat::Iceberg);
    assert_eq!(previous.snapshot_version, 0);
    assert_eq!(previous.files.len(), 1);
    assert_eq!(previous.files[0].size_bytes, Some(f.first_size));
    assert_eq!(previous.log.len(), 1);

    let latest = table::load(
        common::lake(),
        &load_request(f.table.root.to_string_lossy(), None),
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
    assert_eq!(sizes, [f.first_size, f.second_size]);
    let selected = latest.log.iter().find(|c| c.version == 1).unwrap();
    assert!(selected.actions.iter().any(|action| {
        action.kind.as_deref() == Some("delete")
            && action.path.as_deref() == Some("data/deletes.parquet")
    }));
    assert_eq!(latest.log.len(), 2);
}

#[tokio::test]
async fn file_list_walks_the_active_files() {
    let f = Fixture::new();
    let table = v1::Table {
        name: "catalogs/local/schemas/local/tables/events".into(),
        uri: Some(f.metadata.to_string_lossy().into_owned()),
        ..v1::Table::default()
    };
    let mut files: Vec<_> = pqbench::file::list::list(common::lake(), &table, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(files[0].path, "data/first.parquet");
    assert_eq!(files[1].path, "data/second.parquet");
    assert_eq!(
        files[0].name,
        "catalogs/local/schemas/local/tables/events/files/data%2Ffirst.parquet"
    );
    assert_eq!(files[0].size_bytes, Some(f.first_size));
    assert_eq!(files[1].size_bytes, Some(f.second_size));
}

#[tokio::test]
async fn stream_files_resolves_relative_paths_against_the_table_location() {
    let f = Fixture::new();
    let manifest = f.table.manifest(
        "relative.avro",
        &[],
        &[Entry::data(
            "data/first.parquet",
            f.first_size as i64,
            3,
            &[],
        )],
    );
    let list = f.table.list("snap-rel.avro", &[(&manifest, 0, 0, Some(0))]);
    let mut doc: serde_json::Value =
        serde_json::from_slice(&fs::read(&f.metadata).unwrap()).unwrap();
    doc["snapshots"][0]["manifest-list"] = json!(file_uri(&list));
    fs::write(&f.metadata, serde_json::to_vec(&doc).unwrap()).unwrap();

    let files: Vec<_> = table::stream_files(
        common::lake(),
        &load_request(f.metadata.to_string_lossy(), Some(0)),
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
        f.table.root.join("data/first.parquet").to_string_lossy()
    );
}

#[tokio::test]
async fn load_then_bytemass_matches_footer_totals() {
    let f = Fixture::new();
    let info = table::load(
        common::lake(),
        &load_request(f.metadata.to_string_lossy(), None),
    )
    .await
    .unwrap();
    let (mut values, mut bytes) = (0, 0);
    for file in &info.files {
        for col in common::columns(&file.uri().unwrap()).await {
            let mass = col.mass.as_ref().unwrap();
            values += mass.value_count.unwrap_or(0);
            bytes += mass.compressed_bytes.unwrap_or(0);
        }
    }
    assert_eq!(values, 8);
    let expected: u64 = ["data/first.parquet", "data/second.parquet"]
        .iter()
        .map(|rel| read_file_masses(&f.table.root.join(rel)).unwrap().columns[0].compressed_bytes)
        .sum();
    assert_eq!(bytes, expected);
}

#[tokio::test]
async fn rejects_missing_snapshots_changed_files_and_path_escapes() {
    let f = Fixture::new();
    assert!(table::load(
        common::lake(),
        &load_request(f.metadata.to_string_lossy(), Some(99))
    )
    .await
    .is_err());

    let first = f.table.root.join("data/first.parquet");
    fs::write(&first, b"changed").unwrap();
    let info = table::load(
        common::lake(),
        &load_request(f.metadata.to_string_lossy(), Some(0)),
    )
    .await
    .unwrap();
    assert_eq!(info.files[0].size_bytes, Some(f.first_size));

    write_parquet(&first, 3);
    let outside = tempfile::tempdir().unwrap();
    let outside_file = outside.path().join("outside.parquet");
    write_parquet(&outside_file, 2);
    let escaped = f.table.manifest(
        "escape.avro",
        &[],
        &[Entry::data(file_uri(&outside_file), 100, 2, &[])],
    );
    let list = f
        .table
        .list("snap-escape.avro", &[(&escaped, 0, 0, Some(0))]);
    let mut doc: serde_json::Value =
        serde_json::from_slice(&fs::read(&f.metadata).unwrap()).unwrap();
    doc["snapshots"][0]["manifest-list"] = json!(file_uri(&list));
    fs::write(&f.metadata, serde_json::to_vec(&doc).unwrap()).unwrap();
    let error = table::load(
        common::lake(),
        &load_request(f.metadata.to_string_lossy(), Some(0)),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("outside the Iceberg table location"),
        "{error}"
    );
}

#[tokio::test]
async fn partitions_carry_each_transforms_value_and_the_manifests_totals() {
    let t = TestTable::new();
    let uuid = vec![
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0x4d, 0xef, 0x8a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56,
        0x78,
    ];
    let fields = [
        ("day", json!({"type": "int", "logicalType": "date"})),
        ("hour", json!("int")),
        ("region", json!(["null", "string"])),
        (
            "amount",
            json!({"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}),
        ),
        (
            "event_id",
            json!({"type": "fixed", "name": "uuid_fixed", "size": 16}),
        ),
        ("id_bucket", json!("int")),
        ("name_part", json!("string")),
        ("year", json!("int")),
        ("month", json!("int")),
        (
            "ts",
            json!(["null", {"type": "long", "logicalType": "timestamp-micros"}]),
        ),
        (
            "day_identity",
            json!({"type": "int", "logicalType": "date"}),
        ),
    ];
    let row = |region: Avro| {
        vec![
            ("day", Avro::Date(20_089)),
            ("hour", Avro::Int(481_321)),
            ("region", region),
            (
                "amount",
                Avro::Decimal(apache_avro::Decimal::from(vec![0x30, 0x39])),
            ),
            ("event_id", Avro::Fixed(16, uuid.clone())),
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
    };
    let eu_val = || Avro::Union(1, Box::new(Avro::String("eu".into())));
    let m0 = t.manifest(
        "m0.avro",
        &fields,
        &[
            Entry::data("data/a.parquet", 1_000, 10, &row(eu_val())),
            Entry::data("data/b.parquet", 2_000, 20, &row(eu_val())),
            Entry::data(
                "data/c.parquet",
                4_000,
                40,
                &row(Avro::Union(0, Box::new(Avro::Null))),
            ),
        ],
    );
    let s0 = t.list("snap.avro", &[(&m0, 0, 0, Some(7))]);
    let meta = t.write_metadata(
        "v1.metadata.json",
        json!([
            {"id": 1, "name": "id", "required": true, "type": "long"},
            {"id": 2, "name": "region", "type": "string"},
            {"id": 3, "name": "day", "type": "date"},
            {"id": 4, "name": "ts", "type": "timestamptz"},
            {"id": 5, "name": "amount", "type": "decimal(9, 2)"},
            {"id": 6, "name": "event_id", "type": "uuid"},
            {"id": 7, "name": "name", "type": "string"}
        ]),
        json!([{
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
        }]),
        &[(7, None, 1_700_000_000_000, &s0)],
    );

    let records = partitions(&meta, "events").await;
    assert_eq!(records.len(), 2);
    let eu = records
        .iter()
        .find(|p| p.partition_values["region"].as_deref() == Some("eu"))
        .unwrap();
    let null_region = records
        .iter()
        .find(|p| p.partition_values["region"].is_none())
        .unwrap();

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
    .map(|(k, v)| (k.into(), Some(v.into())))
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
    assert!(null_region.name.contains("__HIVE_DEFAULT_PARTITION__"));
}

#[tokio::test]
async fn a_partition_evolved_table_lists_every_specs_values() {
    let mut t = TestTable::new();
    t.default_spec_id = 1;
    let old = t.manifest(
        "m-old.avro",
        &[("day", json!("int"))],
        &[Entry::data(
            "data/old.parquet",
            1_000,
            10,
            &[("day", Avro::Int(20_089))],
        )],
    );
    let new = t.manifest(
        "m-new.avro",
        &[
            ("region", json!(["null", "string"])),
            ("old_day", json!("null")),
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
    let s0 = t.list("snap.avro", &[(&old, 0, 0, Some(7)), (&new, 0, 1, Some(7))]);
    let meta = t.write_metadata(
        "v1.metadata.json",
        json!([
            {"id": 2, "name": "region", "type": "string"},
            {"id": 3, "name": "day", "type": "date"}
        ]),
        json!([
            {"spec-id": 0, "fields": [{"source-id": 3, "field-id": 1000, "name": "day", "transform": "day"}]},
            {"spec-id": 1, "fields": [
                {"source-id": 2, "field-id": 1001, "name": "region", "transform": "identity"},
                {"source-id": 3, "field-id": 1002, "name": "old_day", "transform": "void"}
            ]}
        ]),
        &[(7, None, 1_700_000_000_000, &s0)],
    );

    let info = table::load(common::lake(), &load_request(meta.to_string_lossy(), None))
        .await
        .unwrap();
    assert_eq!(info.partition_columns, ["region", "day"]);
    let records = partitions(&meta, "events").await;
    assert_eq!(records.len(), 2);
    let day = records
        .iter()
        .find(|p| p.partition_values.contains_key("day"))
        .unwrap();
    assert_eq!(day.partition_values["day"].as_deref(), Some("2025-01-01"));
    let region = records
        .iter()
        .find(|p| p.partition_values.contains_key("region"))
        .unwrap();
    assert_eq!(region.partition_values["region"].as_deref(), Some("eu"));
    assert!(!region.partition_values.contains_key("old_day"));
}

#[tokio::test]
async fn delete_files_are_counted_remedied_and_change_the_etag() {
    let mut t = TestTable::new();
    t.format_version = 3;
    let fields = [("region", json!(["null", "string"]))];
    let eu = [(
        "region",
        Avro::Union(1, Box::new(Avro::String("eu".into()))),
    )];
    let data = t.manifest(
        "data.avro",
        &fields,
        &[Entry::data("data/a.parquet", 4_000_000, 40, &eu)],
    );
    let pos = t.manifest(
        "pos.avro",
        &fields,
        &[
            Entry::delete("data/a-del.parquet", 1_500_000, 15, 1, "PARQUET", &eu),
            Entry::delete("data/a-vec.puffin", 400_000, 5, 1, "PUFFIN", &eu),
        ],
    );
    let eq = t.manifest(
        "eq.avro",
        &fields,
        &[Entry::delete("data/a-eq.avro", 200_000, 2, 2, "AVRO", &eu)],
    );
    let all = t.list(
        "all.avro",
        &[
            (&data, 0, 0, Some(7)),
            (&pos, 1, 0, Some(7)),
            (&eq, 1, 0, Some(7)),
        ],
    );
    let fewer = t.list(
        "fewer.avro",
        &[(&data, 0, 0, Some(7)), (&pos, 1, 0, Some(7))],
    );
    let write = |name: &str, list: &Path| {
        t.write_metadata(
            name,
            json!([{"id": 2, "name": "region", "type": "string"}]),
            json!([{"spec-id": 0, "fields": [{"source-id": 2, "name": "region", "transform": "identity"}]}]),
            &[(7, None, 1_700_000_000_000, list)],
        )
    };
    let records = partitions(&write("all.metadata.json", &all), "events").await;
    assert_eq!(records[0].delete_file_count, Some(3));
    assert_eq!(records[0].delete_size_bytes, Some(2_100_000));
    let rule = suggestion(&records[0], pqbench::lint::DELETE_FILES_RULE).unwrap();
    assert!(rule.message.contains("3 delete files") && rule.message.contains("2.0 MiB"));

    let rewritten = partitions(&write("fewer.metadata.json", &fewer), "events").await;
    assert_eq!(rewritten[0].delete_file_count, Some(2));
    assert_ne!(rewritten[0].etag, records[0].etag);
}

#[tokio::test]
async fn superseded_and_deleted_entries_are_removed_files() {
    let t = TestTable::new();
    let fields = [("day", json!("int"))];
    let day = [("day", Avro::Int(20_089))];
    let m0 = t.manifest(
        "m0.avro",
        &fields,
        &[
            Entry::data("data/shared.parquet", 1_000, 10, &day).snapshot(1),
            Entry::data("data/superseded.parquet", 2 * 1024 * 1024, 20, &day).snapshot(1),
        ],
    );
    let m1 = t.manifest(
        "m1.avro",
        &fields,
        &[
            Entry::data("data/shared.parquet", 1_000, 10, &day)
                .status(0)
                .snapshot(1),
            Entry::data("data/superseded.parquet", 2 * 1024 * 1024, 20, &day)
                .status(2)
                .snapshot(2),
            Entry::data("data/current.parquet", 500, 5, &day).snapshot(2),
        ],
    );
    let s1 = t.list("s1.avro", &[(&m0, 0, 0, Some(1))]);
    let s2 = t.list("s2.avro", &[(&m1, 0, 0, Some(2))]);
    let meta = t.write_metadata(
        "v1.metadata.json",
        json!([{"id": 2, "name": "day", "type": "date"}]),
        json!([{"spec-id": 0, "fields": [{"source-id": 2, "name": "day", "transform": "day"}]}]),
        &[
            (1, None, 1_600_000_000_000, &s1),
            (2, Some(1), 1_700_000_000_000, &s2),
        ],
    );

    let records = partitions(&meta, "events").await;
    assert_eq!(records[0].file_count, 2);
    assert_eq!(records[0].size_bytes, 1_500);
    assert_eq!(records[0].record_count, Some(15));
    assert_eq!(records[0].removed_file_count, Some(1));
    assert_eq!(records[0].removed_size_bytes, Some(2 * 1024 * 1024));
    let rule = suggestion(&records[0], pqbench::lint::TOMBSTONES_RULE).unwrap();
    assert_eq!(rule.verified, Some(true));
    assert_eq!(rule.savings_bytes, Some(2 * 1024 * 1024));
}

#[tokio::test]
async fn a_single_snapshot_table_reports_no_removed_files() {
    let t = TestTable::new();
    let m0 = t.manifest(
        "m0.avro",
        &[("day", json!("int"))],
        &[Entry::data(
            "data/a.parquet",
            1_000,
            10,
            &[("day", Avro::Int(20_089))],
        )],
    );
    let s0 = t.list("s0.avro", &[(&m0, 0, 0, Some(7))]);
    let meta = t.write_metadata(
        "v1.metadata.json",
        json!([{"id": 2, "name": "day", "type": "date"}]),
        json!([{"spec-id": 0, "fields": [{"source-id": 2, "name": "day", "transform": "day"}]}]),
        &[(7, None, 1_700_000_000_000, &s0)],
    );
    let records = partitions(&meta, "events").await;
    assert_eq!(records[0].removed_file_count, Some(0));
    assert_eq!(records[0].removed_size_bytes, Some(0));
    assert_eq!(records[0].delete_file_count, None);
    assert!(suggestion(&records[0], pqbench::lint::TOMBSTONES_RULE).is_none());
}

#[tokio::test]
async fn every_known_format_version_loads_and_no_other_does() {
    let mut t = TestTable::new();
    let m0 = t.manifest(
        "m0.avro",
        &[],
        &[Entry::data("data/a.parquet", 1_000, 10, &[])],
    );
    let s0 = t.list("s0.avro", &[(&m0, 0, 0, Some(7))]);
    let mut write = |v: i32| {
        t.format_version = v;
        t.write_metadata(
            &format!("v{v}.metadata.json"),
            json!([{"id": 1, "name": "id", "type": "long"}]),
            json!([{"spec-id": 0, "fields": []}]),
            &[(7, None, 1_700_000_000_000, &s0)],
        )
    };
    for v in [1, 2, 3] {
        let info = table::load(
            common::lake(),
            &load_request(write(v).to_string_lossy(), None),
        )
        .await
        .unwrap();
        assert_eq!(info.files.len(), 1);
    }
    for v in [0, 4] {
        let err = table::load(
            common::lake(),
            &load_request(write(v).to_string_lossy(), None),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("unsupported Iceberg format version"), "{err}");
    }
}

#[test]
fn avro_values_keep_their_types() {
    const SCHEMA: &str = r#"{
      "type": "record", "name": "entry", "fields": [
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
        {"name": "body", "type": {"type": "record", "name": "inner", "fields": [{"name": "leaf", "type": "int"}]}}
      ]
    }"#;
    let schema = Schema::parse_str(SCHEMA).unwrap();
    let mut writer = Writer::new(&schema, Vec::new());
    writer
        .append(Avro::Record(vec![
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
            (
                "amount".into(),
                Avro::Decimal(apache_avro::Decimal::from(vec![0x30, 0x39])),
            ),
            (
                "maybe".into(),
                Avro::Union(1, Box::new(Avro::String("yes".into()))),
            ),
            (
                "body".into(),
                Avro::Record(vec![("leaf".into(), Avro::Int(7))]),
            ),
        ]))
        .unwrap();
    let bytes = writer.into_inner().unwrap();
    let row = &pqbench::third_party::avro::read_avro_values(&bytes, "value").unwrap()[0];
    assert_eq!(row.get("nothing"), Some(&Avro2::Null));
    assert_eq!(row.str("flag"), Some("true"));
    assert_eq!(row.int("small"), Some(-3));
    assert_eq!(row.int("big"), Some(1 << 40));
    assert_eq!(row.str("text"), Some("eu"));
    assert_eq!(row.get("blob"), Some(&Avro2::Bytes(vec![0x0f])));
    assert_eq!(row.str("day"), Some("2025-01-01"));
    assert_eq!(row.str("moment"), Some("2025-01-01T00:00:00Z"));
    assert_eq!(row.str("local"), Some("2025-01-01T00:00:00Z"));
    assert_eq!(row.get("amount"), Some(&Avro2::Bytes(vec![0x30, 0x39])));
    assert_eq!(row.str("maybe"), Some("yes"));
    assert_eq!(row.get("body").unwrap().int("leaf"), Some(7));
}

#[tokio::test]
async fn a_window_selects_the_files_of_the_snapshots_it_names() {
    let t = TestTable::new();
    let fields = [("day", json!("int"))];
    let day = [("day", Avro::Int(20_089))];
    let m0 = t.manifest(
        "m0.avro",
        &fields,
        &[Entry::data("data/old.parquet", 1_000, 10, &day).snapshot(1)],
    );
    let m1 = t.manifest(
        "m1.avro",
        &fields,
        &[Entry::data("data/new.parquet", 2_000, 20, &day).snapshot(2)],
    );
    let s1 = t.list("s1.avro", &[(&m0, 0, 0, Some(1))]);
    let s2 = t.list("s2.avro", &[(&m1, 0, 0, Some(2))]);
    let meta = t.write_metadata(
        "v1.metadata.json",
        json!([{"id": 2, "name": "day", "type": "date"}]),
        json!([{"spec-id": 0, "fields": [{"source-id": 2, "name": "day", "transform": "day"}]}]),
        &[
            (1, None, 1_600_000_000_000, &s1),
            (2, Some(1), 1_700_000_000_000, &s2),
        ],
    );
    let info = table::load(common::lake(), &load_request(meta.to_string_lossy(), None))
        .await
        .unwrap();
    let files: Vec<_> = pqbench::file::list::list(
        common::lake(),
        &info.resource("local", "local", "events"),
        Some(pqbench::time::TimeWindow::day(1_700_000_000_000)),
    )
    .await
    .unwrap()
    .try_collect()
    .await
    .unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "data/new.parquet");
    assert_eq!(files[0].modification_time, Some(1_700_000_000_000));
}

#[tokio::test(flavor = "current_thread")]
async fn v2_manifest_list_with_required_integers_loads() {
    #[derive(Serialize)]
    struct RequiredRow {
        manifest_path: String,
        manifest_length: i64,
        partition_spec_id: i32,
        content: i32,
        added_snapshot_id: i64,
        deleted_files_count: i32,
    }
    let t = TestTable::new();
    let parquet = t.root.join("data/part-0.parquet");
    write_parquet(&parquet, 4);
    let size = fs::metadata(&parquet).unwrap().len() as i64;
    let m0 = t.manifest(
        "m0.avro",
        &[],
        &[Entry::data(file_uri(&parquet), size, 4, &[])],
    );
    let s1 = write_avro(
        &t.meta_dir.join("s1.avro"),
        r#"{"type":"record","name":"manifest_file","fields":[
            {"name":"manifest_path","type":"string"},
            {"name":"manifest_length","type":"long"},
            {"name":"partition_spec_id","type":"int"},
            {"name":"content","type":"int"},
            {"name":"added_snapshot_id","type":"long"},
            {"name":"deleted_files_count","type":"int"}
        ]}"#,
        &[RequiredRow {
            manifest_path: file_uri(&m0),
            manifest_length: fs::metadata(&m0).unwrap().len() as i64,
            partition_spec_id: 0,
            content: 0,
            added_snapshot_id: 1,
            deleted_files_count: 0,
        }],
    );
    let meta = t.write_metadata(
        "v1.metadata.json",
        json!([{"id": 1, "name": "id", "type": "long"}]),
        json!([{"spec-id": 0, "fields": []}]),
        &[(1, None, 1_710_000_000_000, &s1)],
    );
    let listed = partitions(&meta, "events").await;
    assert_eq!(listed[0].file_count, 1);
    assert_eq!(listed[0].record_count, Some(4));
    assert_eq!(
        listed[0].first_file.as_ref().unwrap().modification_time,
        Some(1_710_000_000_000)
    );
}
