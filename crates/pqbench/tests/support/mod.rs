#![allow(dead_code)]

use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use parquet::data_type::Int64Type;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use serde_json::{json, Value};
use tempfile::TempDir;

pub fn write_parquet(path: &Path, rows: i64) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(parse_message_type("message data { REQUIRED INT64 id; }").unwrap());
    let mut writer =
        SerializedFileWriter::new(File::create(path).unwrap(), schema, Default::default()).unwrap();
    let mut group = writer.next_row_group().unwrap();
    let mut column = group.next_column().unwrap().unwrap();
    let values: Vec<_> = (0..rows).collect();
    column
        .typed::<Int64Type>()
        .write_batch(&values, None, None)
        .unwrap();
    column.close().unwrap();
    group.close().unwrap();
    writer.close().unwrap();
}

pub struct Fixture {
    pub directory: TempDir,
}

impl Fixture {
    pub fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let fixture = Self { directory };
        fs::create_dir(fixture.path().join("_delta_log")).unwrap();
        write_parquet(&fixture.path().join("part=a/old file.parquet"), 2);
        write_parquet(&fixture.path().join("part=b/kept.parquet"), 5);
        write_parquet(&fixture.path().join("part=a/added.parquet"), 9);
        fixture.commit(
            0,
            &[
                json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
                metadata(json!({})),
                fixture.add("part=a/old file.parquet", "a", 2),
                fixture.add("part=b/kept.parquet", "b", 5),
            ],
        );
        fixture.commit(
            1,
            &[
                remove("part=a/old file.parquet"),
                fixture.add("part=a/added.parquet", "a", 9),
            ],
        );
        fixture
    }

    pub fn path(&self) -> &Path {
        self.directory.path()
    }

    pub fn add(&self, path: &str, partition: &str, rows: i64) -> Value {
        json!({"add": {
            "path": path.replace(' ', "%20"),
            "partitionValues": {"part": partition},
            "size": fs::metadata(self.path().join(path)).unwrap().len(),
            "modificationTime": 0,
            "dataChange": true,
            "stats": json!({"numRecords": rows}).to_string()
        }})
    }

    pub fn commit(&self, version: u64, actions: &[Value]) {
        let mut text = actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        text.push('\n');
        fs::write(
            self.path().join(format!("_delta_log/{version:020}.json")),
            text,
        )
        .unwrap();
    }

    #[cfg(feature = "delta")]
    pub async fn checkpoint(&self) {
        let url = url::Url::from_directory_path(self.path()).unwrap();
        let table = deltalake::DeltaTableBuilder::from_url(url)
            .unwrap()
            .load()
            .await
            .unwrap();
        deltalake::protocol::checkpoints::create_checkpoint(&table, None)
            .await
            .unwrap();
    }
}

pub fn metadata(configuration: Value) -> Value {
    json!({"metaData": {
        "id": "967e1749-2635-481d-a114-897e027d7000",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": json!({"type": "struct", "fields": [
            {"name":"id","type":"long","nullable":false,"metadata":{}},
            {"name":"part","type":"string","nullable":true,"metadata":{}}
        ]}).to_string(),
        "partitionColumns": ["part"],
        "configuration": configuration,
        "createdTime": 0
    }})
}

pub fn remove(path: &str) -> Value {
    json!({"remove": {"path": path.replace(' ', "%20"), "deletionTimestamp": 1, "dataChange": true}})
}
