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

/// A distinct 32-hex-digit string: unique like a UUID, but not UUID text.
fn hex32(value: u64) -> String {
    let high = mix(value);
    let low = mix(value.wrapping_mul(0xbf58_476d_1ce4_e5b9));
    format!("{high:016x}{low:016x}")
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

/// Write a one-column text file, compressed with zstd.
fn write_values(path: &Path, values: &[String], dictionary: bool, row_group_size: usize) {
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
        row_group_size,
    };
    let bytes = write_parquet(&columns, &rows, &options).expect("write fixture");
    std::fs::write(path, bytes).expect("write fixture file");
}

/// Run a command with an NDJSON standard input and return its NDJSON output.
fn run(args: &[&str], input: &str) -> Vec<Json> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run pqbench");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(input.as_bytes())
        .expect("write input");
    let output = child.wait_with_output().expect("wait for pqbench");
    assert!(
        output.status.success(),
        "{} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson"))
        .collect()
}

/// List a file's potential suggestions.
fn suggestions(path: &Path) -> Vec<Json> {
    run(
        &["list", "suggestions"],
        &crate::common::table_file(path.to_str().expect("utf-8 path")),
    )
}

/// Verify candidates, as `list suggestions | get suggestion` does.
fn verified(records: &[Json]) -> Vec<Json> {
    let input: Vec<String> = records.iter().map(ToString::to_string).collect();
    run(&["get", "suggestion"], &input.join("\n"))
}

#[test]
fn unique_uuid_text_is_a_candidate_then_verified() {
    let directory = temporary("unique");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..100_000).map(uuid).collect();
    write_values(&path, &values, false, 0);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    let finding = &records[0];
    assert_eq!(finding["column"], "id");
    assert_eq!(finding["rule"], "uuid-text");
    assert!(finding.get("verified").is_none(), "{finding}");
    assert!(finding.get("savings_bytes").is_none(), "{finding}");
    assert!(finding["table"].is_string(), "{finding}");
    assert!(finding["size_bytes"].is_u64(), "{finding}");
    assert!(
        finding["writer"]
            .as_str()
            .unwrap()
            .starts_with("parquet-rs"),
        "{finding}"
    );
    assert!(finding["message"].as_str().unwrap().contains("B/value"));

    let measured = verified(&records);
    assert_eq!(measured.len(), 1, "{measured:?}");
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    assert!(measured[0]["savings_bytes"].as_u64().unwrap() >= 64 * 1024);
    assert!(
        measured[0]["message"]
            .as_str()
            .unwrap()
            .contains("smaller at zstd"),
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn repeated_uuid_text_is_measured() {
    let directory = temporary("repeated");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..200_000).map(|value| uuid(value / 2)).collect();
    write_values(&path, &values, true, 0);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    let measured = verified(&records);
    let finding = &measured[0];
    assert_eq!(finding["rule"], "uuid-text");
    match finding["verified"].as_bool() {
        Some(true) => assert!(finding["savings_bytes"].as_u64().unwrap() >= 64 * 1024),
        Some(false) => assert!(finding.get("savings_bytes").is_none()),
        None => panic!("get suggestion must verify or reject: {finding}"),
    }
    assert!(
        finding["message"]
            .as_str()
            .unwrap()
            .contains("controlled rewrite"),
        "{finding}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn non_uuid_text_is_not_reported_as_uuid() {
    let directory = temporary("not-uuid");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..10_000).map(|value| format!("value-{value}")).collect();
    write_values(&path, &values, false, 0);

    let records = suggestions(&path);
    // Nearly-unique non-UUID text still earns the Delta-encoding rule, but the
    // UUID rule must not claim it.
    assert!(
        records.iter().all(|record| record["rule"] != "uuid-text"),
        "{records:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn dictionary_on_unique_strings_is_verified() {
    let directory = temporary("dictionary-unique");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..200_000).map(hex32).collect();
    write_values(&path, &values, true, 20_000);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    let finding = &records[0];
    assert_eq!(finding["rule"], "dictionary-unique");
    assert!(finding.get("savings_bytes").is_none(), "{finding}");
    assert!(
        finding["remedy"]
            .as_str()
            .unwrap()
            .contains("set_column_dictionary_enabled"),
        "{finding}"
    );

    let measured = verified(&records);
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    // Preserving the 10 row groups of 20,000 rows keeps each group's dictionary
    // active in the control, showing the real >64 KiB saving from disabling it.
    assert!(
        measured[0]["savings_bytes"].as_u64().unwrap() >= 64 * 1024,
        "{measured:?}"
    );
    assert!(
        measured[0]["message"]
            .as_str()
            .unwrap()
            .contains("smaller at zstd"),
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn dictionary_on_repeated_strings_is_not_reported() {
    let directory = temporary("dictionary-repeated");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..100_000).map(|value| hex32(value % 100)).collect();
    write_values(&path, &values, true, 20_000);

    let records = suggestions(&path);
    assert!(records.is_empty(), "{records:?}");

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn a_stale_candidate_is_rejected_not_dropped() {
    let directory = temporary("stale");
    let path = directory.join("data.parquet");
    let unique: Vec<String> = (0..200_000).map(hex32).collect();
    write_values(&path, &unique, true, 20_000);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");

    let repeated: Vec<String> = (0..100_000).map(|value| hex32(value % 100)).collect();
    write_values(&path, &repeated, true, 20_000);

    let measured = verified(&records);
    assert_eq!(measured.len(), 1, "{measured:?}");
    assert_eq!(measured[0]["verified"], false, "{measured:?}");
    assert!(measured[0].get("savings_bytes").is_none(), "{measured:?}");
    assert!(
        measured[0]["message"]
            .as_str()
            .unwrap()
            .contains("no longer detects"),
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// The sample is one representative whole row group: a short trailing group is
/// never the one that stands for the file, and `get suggestion` re-reads it.
#[test]
fn a_sample_is_one_representative_row_group() {
    let directory = temporary("row-groups");
    let path = directory.join("data.parquet");
    // 25 rows in groups of 10, 10, and a 5-row tail: the full groups are
    // closest to the average, so the first one is the sample.
    let values: Vec<String> = (0..25).map(uuid).collect();
    write_values(&path, &values, false, 10);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["sample_row_count"], 10, "{records:?}");
    assert_eq!(records[0]["sample_row_group_count"], 1, "{records:?}");

    let measured = verified(&records);
    assert_eq!(measured[0]["sample_row_count"], 10, "{measured:?}");
    assert_eq!(measured[0]["sample_row_group_count"], 1, "{measured:?}");

    let _ = std::fs::remove_dir_all(directory);
}

/// A file smaller than the old row limit reports the rows it decoded, not the
/// limit it asked for.
#[test]
fn a_small_file_reports_the_rows_it_decoded() {
    let directory = temporary("small");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..8).map(uuid).collect();
    write_values(&path, &values, false, 0);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["sample_row_count"], 8, "{records:?}");
    assert_eq!(records[0]["sample_row_group_count"], 1, "{records:?}");

    let _ = std::fs::remove_dir_all(directory);
}

/// `download` copies a file, and the commands read the copy with `--local`
/// while their records keep naming the original: the original is gone when
/// they run.
#[test]
fn a_downloaded_copy_is_read_under_the_original_name() {
    let directory = temporary("local");
    let path = directory.join("data.parquet");
    let copy = directory.join("copy.parquet");
    let values: Vec<String> = (0..100_000).map(uuid).collect();
    write_values(&path, &values, false, 0);
    let record = crate::common::table_file(path.to_str().expect("utf-8 path"));
    run(
        &["download", "--output", copy.to_str().expect("utf-8 path")],
        &record,
    );
    assert_eq!(std::fs::read(&copy).unwrap(), std::fs::read(&path).unwrap());
    let expected = run(&["list", "suggestions"], &record);
    std::fs::remove_file(&path).unwrap();

    let local = copy.to_str().expect("utf-8 path");
    let records = run(&["list", "suggestions", "--local", local], &record);
    assert_eq!(records, expected);
    let columns = run(&["bytemass", "--local", local], &record);
    assert!(!columns.is_empty());
    let blob = columns[0]["name"].as_str().unwrap();
    assert!(blob.ends_with("data.parquet"), "{blob}");
    let input: Vec<String> = records.iter().map(ToString::to_string).collect();
    let measured = run(&["get", "suggestion", "--local", local], &input.join("\n"));
    assert_eq!(measured[0]["verified"], true, "{measured:?}");

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn a_remote_blob_with_a_local_copy_verifies_without_network() {
    let directory = temporary("remote-local");
    let copy = directory.join("copy.parquet");
    let values: Vec<String> = (0..100_000).map(uuid).collect();
    write_values(&copy, &values, false, 0);

    let blob = pqbench::v1::name::blob("s3", "example-bucket/prefix/data.parquet");
    let remote_record = serde_json::json!({
        "name": "catalogs/demo/schemas/s/tables/t/files/f1",
        "blob": blob,
    })
    .to_string();
    let local = copy.to_str().expect("utf-8 path");
    let records = run(&["list", "suggestions", "--local", local], &remote_record);
    assert_eq!(records.len(), 1, "{records:?}");
    assert!(
        records[0]["name"].as_str().unwrap().starts_with(&blob),
        "{records:?}"
    );
    let measured = run(
        &["get", "suggestion", "--local", local],
        &records[0].to_string(),
    );
    assert_eq!(measured[0]["verified"], true, "{measured:?}");

    let _ = std::fs::remove_dir_all(directory);
}
