use std::process::{Command, Stdio};

// `small_reddit_none.parquet` is a 3000-row NONE-compressed subset of the
// MIT-licensed reddit_dataset_90 (goldentraversy07/reddit_dataset_90).

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

fn ndjson_records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

/// The column a `…/columns/{column}/bytemass` name measures, decoded from the
/// name's `columns` segment.
fn column_name(record: &serde_json::Value) -> Option<String> {
    pqbench::v1::name::parse_bytemass(record["name"].as_str()?).map(|(_, column)| column)
}

/// Whether the record's `name` is a `Blob`.
fn is_blob(record: &serde_json::Value) -> bool {
    record["name"]
        .as_str()
        .is_some_and(|name| pqbench::v1::name::parse_blob(name).is_some())
}

/// End-to-end: a pipe streams one `Blob` per file, then one `Bytemass` per
/// column.
#[test]
fn bytemass_streams_column_rows() {
    let exe = env!("CARGO_BIN_EXE_pqbench");
    let out = Command::new(exe)
        .args(["bytemass", parquet_fixture()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let records = ndjson_records(&out.stdout);

    let first = &records[0];
    assert!(is_blob(first), "first record is not a Blob: {first}");
    assert!(
        first["name"].as_str().unwrap().starts_with("blobs/"),
        "{first}"
    );
    assert_eq!(first["column_count"], 7);
    assert!(first["row_group_count"].as_u64().unwrap() >= 1);

    let columns: Vec<String> = records.iter().filter_map(column_name).collect();
    for column in [
        "url_encoded",
        "text",
        "username_encoded",
        "label",
        "communityName",
        "dataType",
        "datetime",
    ] {
        assert!(
            columns.iter().any(|name| name == column),
            "missing {column}"
        );
    }

    // Every column carries positive bytes and values.
    for record in records
        .iter()
        .filter(|record| column_name(record).is_some())
    {
        assert!(record["compressed_bytes"].as_u64().unwrap() > 0, "{record}");
        assert!(record["value_count"].as_u64().unwrap() > 0, "{record}");
    }

    let last = records.last().unwrap();
    assert!(
        column_name(last).is_some(),
        "last record is not a Bytemass: {last}"
    );
}

/// End-to-end: `--json` is the same NDJSON stream.
#[test]
fn bytemass_json_is_the_stream() {
    let exe = env!("CARGO_BIN_EXE_pqbench");
    let out = Command::new(exe)
        .args(["bytemass", parquet_fixture()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let records = ndjson_records(&out.stdout);
    assert!(is_blob(&records[0]), "{:?}", records[0]);
    assert!(records.iter().any(|record| column_name(record).is_some()));
    assert!(!out.stdout.windows(10).any(|w| w == b"\"children\""));
}

#[test]
fn bytemass_rows_include_footer_facts() {
    let exe = env!("CARGO_BIN_EXE_pqbench");
    let out = Command::new(exe)
        .args(["bytemass", parquet_fixture()])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows: Vec<_> = ndjson_records(&out.stdout)
        .into_iter()
        .filter(|record| column_name(record).is_some())
        .collect();
    assert!(!rows.is_empty());
    assert!(rows
        .iter()
        .all(|row| !row["encodings"].as_array().unwrap().is_empty()));
    assert!(rows
        .iter()
        .all(|row| row["value_count"].as_u64().unwrap() > 0));
}

/// End-to-end: a reader that closes early ends the stream, not the command.
#[test]
fn bytemass_exits_cleanly_when_stdout_is_closed() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pqbench"));
    command
        .arg("bytemass")
        .arg(parquet_fixture())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    drop(child.stdout.take());
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
