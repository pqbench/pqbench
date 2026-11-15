//! Blackbox tests for `list suggestions`' UUID rule.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use pqbench::third_party::parquet::api::{write_parquet, Kind, TypedColumn, Value, WriteOptions};
use serde_json::Value as Json;

fn uuid(value: u64) -> String {
    let high = mix(value);
    let low = mix(value ^ 0x9e37_79b9_7f4a_7c15);
    let text = format!("{high:016x}{low:016x}");
    format!(
        "{}-{}-{}-{}-{}",
        &text[0..8],
        &text[8..12],
        &text[12..16],
        &text[16..20],
        &text[20..32]
    )
}

/// A deterministic 64-bit mixing function, so the fixture's UUIDs are random
/// enough that text and binary compress on their own terms.
fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn temporary(name: &str) -> PathBuf {
    let directory =
        std::env::temp_dir().join(format!("pqbench-suggestions-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).expect("temporary directory");
    directory
}

/// Write a one-column UUID text file, compressed with zstd.
fn write_uuids(path: &Path, values: &[String], dictionary: bool) {
    let rows: Vec<Vec<Value>> = values
        .iter()
        .map(|value| vec![Value::Bytes(value.as_bytes().to_vec())])
        .collect();
    let columns = [TypedColumn {
        name: "id".into(),
        kind: Kind::Bytes,
    }];
    let options = WriteOptions {
        compression: "zstd".into(),
        level: Some(3),
        dictionary,
        dictionary_bytes: None,
        encoding: None,
        page_size: None,
        sorting_columns: Vec::new(),
        row_group_size: 0,
    };
    let bytes = write_parquet(&columns, &rows, &options).expect("write fixture");
    std::fs::write(path, bytes).expect("write fixture file");
}

/// Feed one table-file record to `list suggestions` and return its NDJSON.
fn suggestions(path: &Path) -> Vec<Json> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .args(["list", "suggestions"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run pqbench list suggestions");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(crate::common::table_file(path.to_str().expect("utf-8 path")).as_bytes())
        .expect("write table file");
    let output = child.wait_with_output().expect("wait for pqbench");
    assert!(
        output.status.success(),
        "list suggestions failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson"))
        .collect()
}

fn uuid_suggestions(records: &[Json]) -> Vec<&Json> {
    records
        .iter()
        .filter(|record| record["rule"] == "uuid-text")
        .collect()
}

fn report(records: &[Json]) -> &Json {
    records
        .iter()
        .find(|record| {
            record["name"]
                .as_str()
                .is_some_and(|name| name.ends_with("/suggestions"))
        })
        .expect("suggestion report")
}

#[test]
fn unique_uuid_text_is_reported_with_savings() {
    let directory = temporary("unique");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..100_000).map(uuid).collect();
    write_uuids(&path, &values, false);

    let records = suggestions(&path);
    let findings = uuid_suggestions(&records);
    assert_eq!(findings.len(), 1, "{records:?}");
    let finding = findings[0];
    assert_eq!(finding["column"], "id");
    assert!(finding["savings_bytes"].as_u64().unwrap() > 0, "{finding}");
    assert!(
        finding["message"].as_str().unwrap().contains("prices at"),
        "{finding}"
    );

    let report = report(&records);
    assert_eq!(report["suggestion_count"], 1);
    assert_eq!(report["potential_savings_bytes"], finding["savings_bytes"]);

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn repeated_uuid_text_is_priced_below_the_binary_floor() {
    let directory = temporary("repeated");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..200_000).map(|value| uuid(value / 2)).collect();
    write_uuids(&path, &values, true);

    let records = suggestions(&path);
    let findings = uuid_suggestions(&records);
    assert_eq!(findings.len(), 1, "{records:?}");
    let finding = findings[0];
    let savings = finding["savings_bytes"].as_u64().unwrap();
    assert!(savings >= 64 * 1024, "{finding}");
    assert!(
        finding["message"].as_str().unwrap().contains("prices at"),
        "{finding}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn non_uuid_text_is_not_reported() {
    let directory = temporary("not-uuid");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..10_000).map(|value| format!("value-{value}")).collect();
    write_uuids(&path, &values, false);

    let records = suggestions(&path);
    assert!(uuid_suggestions(&records).is_empty(), "{records:?}");
    assert_eq!(report(&records)["suggestion_count"], 0);

    let _ = std::fs::remove_dir_all(directory);
}
