use pqbench::dump::{self, DumpFile, DumpRequest};

fn parquet_fixture() -> String {
    format!(
        "{}/tests/fixtures/small_reddit_none.parquet",
        env!("CARGO_MANIFEST_DIR")
    )
}

#[tokio::test]
async fn dump_reads_rows_from_a_local_file() {
    let dump = dump::dump(&DumpRequest {
        files: vec![DumpFile {
            path: "reddit.parquet".into(),
            uri: parquet_fixture(),
            table: None,
        }],
    })
    .await
    .unwrap();
    assert_eq!(dump.columns[0], "_path");
    assert!(dump.columns.iter().any(|column| column == "url_encoded"));
    assert_eq!(dump.rows.len(), 3000);
    assert_eq!(dump.rows[0][0], "reddit.parquet");
    let csv = dump::render_csv(&dump);
    assert!(csv.starts_with("_path,"));
    assert!(csv.contains("url_encoded"));
    let json = dump::render_json(&dump).unwrap();
    let first: serde_json::Value = serde_json::from_str(json.lines().next().unwrap()).unwrap();
    assert_eq!(first["_path"], "reddit.parquet");
    assert!(first.get("url_encoded").is_some());
}

#[tokio::test]
async fn dump_rejects_empty_files() {
    let error = dump::dump(&DumpRequest { files: Vec::new() })
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no files"), "{error}");
}
