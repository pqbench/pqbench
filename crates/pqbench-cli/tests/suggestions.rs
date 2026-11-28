//! Blackbox tests for the column rules `list suggestions` detects and
//! `get suggestion` measures.

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

/// The writer settings a fixture is written with: zstd at the common default,
/// with the dictionary and statistics settings a test needs.
fn options(dictionary: bool) -> WriteOptions {
    WriteOptions {
        compression: "zstd".into(),
        level: Some(3),
        dictionary,
        dictionary_bytes: None,
        encoding: None,
        page_size: None,
        stats_truncate_length: None,
        sorting_columns: Vec::new(),
        row_group_size: 0,
    }
}

/// Write one `id` column of `rows` and return the file's path.
fn write_column(path: &Path, column: TypedColumn, rows: &[Vec<Value>], options: WriteOptions) {
    let bytes = write_parquet(&[column], rows, &options).expect("write fixture");
    std::fs::write(path, bytes).expect("write fixture file");
}

/// Write a one-column text file, compressed with zstd.
fn write_values(path: &Path, values: &[String], dictionary: bool, row_group_size: usize) {
    let rows: Vec<Vec<Value>> = values
        .iter()
        .map(|value| vec![Value::Bytes(value.as_bytes().to_vec())])
        .collect();
    let column = TypedColumn {
        name: "id".into(),
        kind: Kind::Bytes,
    };
    let options = WriteOptions {
        row_group_size,
        ..options(dictionary)
    };
    write_column(path, column, &rows, options);
}

/// Write a one-column integer file, compressed with zstd.
fn write_integers(path: &Path, values: &[i64], dictionary: bool) {
    let rows: Vec<Vec<Value>> = values
        .iter()
        .map(|value| vec![Value::Integer(*value)])
        .collect();
    write_column(
        path,
        TypedColumn {
            name: "id".into(),
            kind: Kind::Integer,
        },
        &rows,
        options(dictionary),
    );
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

/// Run a command and return its standard output as text, for the human table.
fn run_text(args: &[&str], input: &str) -> String {
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
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Verify candidates, as `list suggestions | get suggestion` does.
fn verified(records: &[Json]) -> Vec<Json> {
    let input: Vec<String> = records.iter().map(ToString::to_string).collect();
    run(&["get", "suggestion"], &input.join("\n"))
}

#[test]
fn a_local_path_lints_verifies_and_formats_as_a_pipe() {
    let directory = temporary("path");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..100_000).map(uuid).collect();
    write_values(&path, &values, false, 0);

    let text = path.to_str().expect("utf-8 path");
    let records = run(&["list", "suggestions", text], "");
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["rule"], "uuid-text", "{records:?}");
    // The path is the minimal table-file record a `table-file` stage names.
    assert_eq!(
        records[0]["name"],
        format!(
            "{}/columns/id/suggestions/uuid-text",
            pqbench::v1::name::blob_name(text)
        ),
        "{records:?}"
    );
    assert_eq!(
        records[0]["size_bytes"],
        std::fs::metadata(&path).unwrap().len()
    );
    let remedies = records[0]["remedies"].as_array().expect("remedies");
    assert!(!remedies.is_empty(), "{records:?}");
    assert!(
        remedies
            .iter()
            .any(|remedy| remedy["engine"] == "pyarrow" && remedy["setting"].is_string()),
        "{records:?}"
    );

    // The same pipeline a table-file stream takes: the candidate verifies, and
    // the verified record formats as one table row.
    let verified = verified(&records);
    assert_eq!(verified[0]["verified"], true, "{verified:?}");
    let input: Vec<String> = verified.iter().map(ToString::to_string).collect();
    let table = run_text(&["format"], &input.join("\n"));
    assert!(table.contains("uuid-text"), "{table}");
    assert!(table.contains("yes"), "{table}");

    let _ = std::fs::remove_dir_all(directory);
}

#[test]
fn a_missing_path_is_reported_and_fails_the_run() {
    let directory = temporary("missing");
    let missing = directory.join("absent.parquet");

    let child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .args(["list", "suggestions"])
        .arg(&missing)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run pqbench");
    let output = child.wait_with_output().expect("wait for pqbench");
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("absent.parquet"), "{stderr}");

    let _ = std::fs::remove_dir_all(directory);
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

/// Dense sequential integers without Delta binary packing are a candidate, and
/// the controlled rewrite confirms the saving.
#[test]
fn dense_integers_earn_delta_binary_packing() {
    let directory = temporary("delta-integer");
    let path = directory.join("data.parquet");
    let values: Vec<i64> = (0..200_000).collect();
    write_integers(&path, &values, false);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["rule"], "delta-integer", "{records:?}");
    assert_eq!(records[0]["column"], "id");
    assert!(
        records[0]["message"]
            .as_str()
            .unwrap()
            .contains("DELTA_BINARY_PACKED"),
        "{records:?}"
    );
    let remedies = records[0]["remedies"].as_array().expect("remedies");
    assert!(
        remedies
            .iter()
            .any(|remedy| remedy["engine"] == "parquet-rs"
                && remedy["setting"]
                    .as_str()
                    .unwrap()
                    .contains("DELTA_BINARY_PACKED")),
        "{records:?}"
    );

    let measured = verified(&records);
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    assert!(
        measured[0]["savings_bytes"].as_u64().unwrap() >= 64 * 1024,
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// Dense integers written with a dictionary that overflowed prefer packed
/// deltas: the larger win covers the encoding, not just the dictionary.
#[test]
fn a_dictionary_on_dense_integers_prefers_delta_packing() {
    let directory = temporary("delta-integer-dictionary");
    let path = directory.join("data.parquet");
    let values: Vec<i64> = (0..200_000).collect();
    write_integers(&path, &values, true);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["rule"], "delta-integer", "{records:?}");

    let measured = verified(&records);
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    assert!(
        measured[0]["savings_bytes"].as_u64().unwrap() >= 64 * 1024,
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// Random integers have no win to make, so the rule must not claim one.
#[test]
fn random_integers_are_not_delta_encoded() {
    let directory = temporary("delta-integer-random");
    let path = directory.join("data.parquet");
    let values: Vec<i64> = (0..200_000).map(|value| mix(value).cast_signed()).collect();
    write_integers(&path, &values, false);

    let records = suggestions(&path);
    assert!(
        records
            .iter()
            .all(|record| record["rule"] != "delta-integer"),
        "{records:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// Repeated integers without a dictionary are a candidate for one, and the
/// control prices the dictionary on integers rather than on text.
#[test]
fn repeated_integers_earn_a_dictionary() {
    let directory = temporary("dictionary-integer");
    let path = directory.join("data.parquet");
    // 1,000 distinct values with no byte pattern: PLAIN cannot compress them,
    // a dictionary's indices can.
    let distinct: Vec<i64> = (0..1_000u64).map(|value| mix(value) as i64).collect();
    let values: Vec<i64> = (0..400_000).map(|value| distinct[value % 1_000]).collect();
    write_integers(&path, &values, false);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["rule"], "dictionary-missing", "{records:?}");
    assert!(
        records[0]["message"].as_str().unwrap().contains("INT64"),
        "{records:?}"
    );

    let measured = verified(&records);
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    assert!(
        measured[0]["savings_bytes"].as_u64().unwrap() >= 64 * 1024,
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// A chunk that outgrew its dictionary page and fell back to PLAIN is priced
/// three ways, and the winner is the one reported.
#[test]
fn a_dictionary_fallback_is_priced() {
    let directory = temporary("dictionary-fallback");
    let path = directory.join("data.parquet");
    // 1,000 distinct incompressible values over 200,000 rows: the chunk keeps a
    // dictionary page while overflowing it, the values repeat too much for the
    // dictionary rules to claim them, and raising the limit pays.
    let distinct: Vec<String> = (0..1_000u64)
        .map(|value| format!("{:016x}{:016x}", mix(value), mix(value ^ 7)))
        .collect();
    let values: Vec<String> = (0..200_000)
        .map(|value| distinct[value % 1_000].clone())
        .collect();
    let rows: Vec<Vec<Value>> = values
        .iter()
        .map(|value| vec![Value::Bytes(value.as_bytes().to_vec())])
        .collect();
    let options = WriteOptions {
        dictionary_bytes: Some(1_024),
        ..options(true)
    };
    write_column(
        &path,
        TypedColumn {
            name: "id".into(),
            kind: Kind::Bytes,
        },
        &rows,
        options,
    );

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["rule"], "dictionary-fallback", "{records:?}");
    assert!(
        records[0]["message"]
            .as_str()
            .unwrap()
            .contains("fell back to PLAIN"),
        "{records:?}"
    );
    let remedies = records[0]["remedies"].as_array().expect("remedies");
    assert!(
        remedies.iter().any(|remedy| remedy["engine"] == "pyarrow"
            && remedy["setting"]
                .as_str()
                .unwrap()
                .contains("dictionary_pagesize_limit")),
        "{records:?}"
    );

    let measured = verified(&records);
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    assert!(
        measured[0]["savings_bytes"].as_u64().unwrap() >= 64 * 1024,
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// A dictionary that never fell back is not a fallback candidate.
#[test]
fn a_dictionary_without_a_fallback_is_not_reported() {
    let directory = temporary("dictionary-no-fallback");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..100_000)
        .map(|value| format!("prefix-{:06}", value % 60_000))
        .collect();
    write_values(&path, &values, true, 0);

    let records = suggestions(&path);
    assert!(
        records
            .iter()
            .all(|record| record["rule"] != "dictionary-fallback"),
        "{records:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// Untruncated footer bounds are priced by arithmetic on the footer's own
/// bytes, with no re-encode.
#[test]
fn untruncated_statistics_are_priced() {
    let directory = temporary("stats-truncated");
    let path = directory.join("data.parquet");
    // A few repeated long values: the chunk keeps a dictionary, and each bound
    // is far longer than the 64-byte truncation length.
    let values: Vec<String> = (0..100_000)
        .map(|value| format!("{}-{}", "a".repeat(300), value % 8))
        .collect();
    let rows: Vec<Vec<Value>> = values
        .iter()
        .map(|value| vec![Value::Bytes(value.as_bytes().to_vec())])
        .collect();
    let options = WriteOptions {
        // A writer with no truncation length, as Arrow C++ writes them.
        stats_truncate_length: Some(1 << 20),
        ..options(true)
    };
    write_column(
        &path,
        TypedColumn {
            name: "id".into(),
            kind: Kind::Bytes,
        },
        &rows,
        options,
    );

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["rule"], "stats-truncated", "{records:?}");
    assert!(
        records[0]["message"]
            .as_str()
            .unwrap()
            .contains("above the 64-byte truncation length"),
        "{records:?}"
    );

    let measured = verified(&records);
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    assert!(
        measured[0]["savings_bytes"].as_u64().unwrap() > 0,
        "{measured:?}"
    );
    assert!(
        measured[0]["message"]
            .as_str()
            .unwrap()
            .contains("footer's recorded bounds"),
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// Truncated footer bounds are no candidate: the writer already stopped at the
/// 64-byte length.
#[test]
fn truncated_statistics_are_not_reported() {
    let directory = temporary("stats-truncated-default");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..100_000)
        .map(|value| format!("{}-{}", "a".repeat(300), value % 8))
        .collect();
    write_values(&path, &values, true, 0);

    let records = suggestions(&path);
    assert!(
        records
            .iter()
            .all(|record| record["rule"] != "stats-truncated"),
        "{records:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// ISO-8601 timestamp text is a candidate for a microsecond `INT64`, and the
/// controlled rewrite confirms the saving.
#[test]
fn timestamp_text_earns_microsecond_integers() {
    let directory = temporary("text-timestamp");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..200_000)
        .map(|value| {
            let seconds = 1_700_000_000 + value;
            format!(
                "2023-11-14T{:02}:{:02}:{:02}.{:03}Z",
                seconds / 3_600 % 24,
                seconds / 60 % 60,
                seconds % 60,
                value % 1_000
            )
        })
        .collect();
    write_values(&path, &values, false, 0);

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["rule"], "text-timestamp", "{records:?}");
    assert!(
        records[0]["message"]
            .as_str()
            .unwrap()
            .contains("ISO-8601 timestamp text"),
        "{records:?}"
    );

    let measured = verified(&records);
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    assert!(measured[0]["savings_bytes"].as_u64().unwrap() >= 64 * 1024);

    let _ = std::fs::remove_dir_all(directory);
}

/// Text that is not entirely timestamps never earns the timestamp rule.
#[test]
fn mixed_text_is_not_a_timestamp_column() {
    let directory = temporary("text-not-timestamp");
    let path = directory.join("data.parquet");
    let values: Vec<String> = (0..100_000)
        .map(|value| {
            if value % 1_000 == 0 {
                format!("not-a-timestamp-{value}")
            } else {
                format!("2023-11-14T12:00:{:02}Z", value % 60)
            }
        })
        .collect();
    write_values(&path, &values, false, 0);

    let records = suggestions(&path);
    assert!(
        records
            .iter()
            .all(|record| record["rule"] != "text-timestamp"),
        "{records:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// A chunk split into many tiny data pages is a candidate for the standard
/// 1 MiB page size, and the controlled rewrite proves the saving.
#[test]
fn tiny_data_pages_earn_a_larger_page_size() {
    let directory = temporary("page-size");
    let path = directory.join("data.parquet");
    let values: Vec<i64> = (0..200_000).map(|value| mix(value).cast_signed()).collect();
    let rows: Vec<Vec<Value>> = values
        .iter()
        .map(|value| vec![Value::Integer(*value)])
        .collect();
    let options = WriteOptions {
        page_size: Some(4 * 1024),
        ..options(false)
    };
    write_column(
        &path,
        TypedColumn {
            name: "id".into(),
            kind: Kind::Integer,
        },
        &rows,
        options,
    );

    let records = suggestions(&path);
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["rule"], "page-size", "{records:?}");
    assert_eq!(records[0]["column"], "id");
    assert!(
        records[0]["message"]
            .as_str()
            .unwrap()
            .contains("data pages average"),
        "{records:?}"
    );
    let remedies = records[0]["remedies"].as_array().expect("remedies");
    assert!(
        remedies.iter().any(|remedy| remedy["engine"] == "pyarrow"
            && remedy["setting"]
                .as_str()
                .unwrap()
                .contains("data_page_size=1048576")),
        "{records:?}"
    );
    assert!(
        remedies
            .iter()
            .any(|remedy| remedy["engine"] == "parquet-rs"
                && remedy["setting"]
                    .as_str()
                    .unwrap()
                    .contains("set_data_page_size_limit(1048576)")),
        "{records:?}"
    );

    let measured = verified(&records);
    assert_eq!(measured[0]["verified"], true, "{measured:?}");
    assert!(
        measured[0]["savings_bytes"].as_u64().unwrap() > 0,
        "{measured:?}"
    );
    assert!(
        measured[0]["message"]
            .as_str()
            .unwrap()
            .contains("controlled rewrite"),
        "{measured:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}

/// The same values at parquet-rs's standard 1 MiB page size are not split into
/// tiny pages, so the rule must not claim them.
#[test]
fn standard_page_sizes_are_not_reported() {
    let directory = temporary("page-size-standard");
    let path = directory.join("data.parquet");
    let values: Vec<i64> = (0..200_000).map(|value| mix(value).cast_signed()).collect();
    let rows: Vec<Vec<Value>> = values
        .iter()
        .map(|value| vec![Value::Integer(*value)])
        .collect();
    write_column(
        &path,
        TypedColumn {
            name: "id".into(),
            kind: Kind::Integer,
        },
        &rows,
        options(false),
    );

    let records = suggestions(&path);
    assert!(
        records.iter().all(|record| record["rule"] != "page-size"),
        "{records:?}"
    );

    let _ = std::fs::remove_dir_all(directory);
}
