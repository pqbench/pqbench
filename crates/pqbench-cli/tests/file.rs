#![cfg(feature = "delta")]

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::json;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn pipe(args: &[&str], stdin: &str) -> std::process::Output {
    let mut child = pqbench()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

/// The resource name a local Delta path gets under `local.local`.
fn table_name(path: &str) -> String {
    let table = std::path::Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("a fixture path has a final segment");
    format!("catalogs/local/schemas/local/tables/{table}")
}

/// The table record a local Delta path names, under `local.local`.
fn table_record(path: &str) -> String {
    format!(
        "{}\n",
        json!({
            "name": table_name(path),
            "uri": path,
        })
    )
}

fn ndjson_records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

struct DeltaFixture {
    // Held only to keep the temporary directory alive for the test's duration.
    // aipnaming: allow(aip-140/underscores)
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
}

/// The log's header: protocol and metadata for one `id: long` column.
fn header() -> Vec<serde_json::Value> {
    vec![
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}),
        json!({"metaData": {
            "id": "11111111-1111-1111-1111-111111111111",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0
        }}),
    ]
}

/// One add action for a fixture file, dated `modification_time`.
fn add(root: &std::path::Path, path: &str, modification_time: i64) -> serde_json::Value {
    std::fs::copy(parquet_fixture(), root.join(path)).unwrap();
    let size = std::fs::metadata(root.join(path)).unwrap().len();
    json!({"add": {
        "path": path,
        "partitionValues": {},
        "size": size,
        "modificationTime": modification_time,
        "dataChange": true,
        "stats": "{\"numRecords\":3000,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":1},\"nullCount\":{\"id\":0},\"tightBounds\":true}"
    }})
}

/// A Delta table whose one commit adds `files` with the dates given.
fn delta_fixture(files: &[(&str, i64)]) -> DeltaFixture {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_path_buf();
    std::fs::create_dir(root.join("_delta_log")).unwrap();
    let mut actions = header();
    for (path, time) in files {
        actions.push(add(&root, path, *time));
    }
    actions.push(json!({"commitInfo": {"timestamp": 1_700_000_000_000i64, "operation": "WRITE"}}));
    let text = actions
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(root.join("_delta_log/00000000000000000000.json"), text).unwrap();
    DeltaFixture {
        _directory: directory,
        path: root,
    }
}

/// `list files` walks a table's active files and names each one.
#[test]
fn file_ls_lists_the_snapshots_active_files() {
    let fixture = delta_fixture(&[("data.parquet", 0)]);
    let files = pipe(
        &["list", "files"],
        &table_record(fixture.path.to_str().unwrap()),
    );
    assert!(
        files.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&files.stderr)
    );
    let records = ndjson_records(&files.stdout);
    assert_eq!(records.len(), 1);
    let name = records[0]["name"].as_str().unwrap();
    assert!(
        name.starts_with("catalogs/local/schemas/local/tables/"),
        "{name}"
    );
    assert!(name.ends_with("/files/data.parquet"), "{name}");
    let blob = records[0]["blob"].as_str().unwrap();
    assert!(blob.starts_with("blobs/file/"), "{blob}");
    assert!(blob.ends_with("data.parquet"), "{blob}");
}

/// Every table on the stream is walked, and each file is named under its own
/// table whichever walk finishes first.
#[test]
fn file_ls_walks_every_table_on_the_stream() {
    let first = delta_fixture(&[("a.parquet", 0)]);
    let second = delta_fixture(&[("b.parquet", 0)]);
    let stdin = format!(
        "{}{}",
        table_record(first.path.to_str().unwrap()),
        table_record(second.path.to_str().unwrap())
    );
    let files = pipe(&["list", "files"], &stdin);
    assert!(
        files.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&files.stderr)
    );
    let mut names: Vec<String> = ndjson_records(&files.stdout)
        .iter()
        .map(|record| record["name"].as_str().unwrap().to_owned())
        .collect();
    names.sort();
    let mut expected = [
        format!(
            "{}/files/a.parquet",
            table_name(first.path.to_str().unwrap())
        ),
        format!(
            "{}/files/b.parquet",
            table_name(second.path.to_str().unwrap())
        ),
    ];
    expected.sort();
    assert_eq!(names, expected);
}

/// `--last` and `--partition` keep only the files the log dates in the window,
/// so a record in hand can be narrowed without re-reading the table.
#[test]
fn file_ls_keeps_only_the_window_it_is_given() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let old = now - 40 * 86_400_000;
    let fixture = delta_fixture(&[("old.parquet", old), ("new.parquet", now)]);

    let recent = pipe(
        &["list", "files", "--last", "1d"],
        &table_record(fixture.path.to_str().unwrap()),
    );
    assert!(
        recent.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&recent.stderr)
    );
    let records = ndjson_records(&recent.stdout);
    assert_eq!(
        records.len(),
        1,
        "{}",
        String::from_utf8_lossy(&recent.stdout)
    );
    assert!(records[0]["name"]
        .as_str()
        .unwrap()
        .ends_with("/files/new.parquet"));

    // The older file's day: `--partition` is one UTC day, not a lookback.
    let day = (old / 86_400_000) * 86_400_000;
    let iso = civil_date(day);
    let older = pipe(
        &["list", "files", "--partition", &iso],
        &table_record(fixture.path.to_str().unwrap()),
    );
    assert!(
        older.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&older.stderr)
    );
    let records = ndjson_records(&older.stdout);
    assert_eq!(
        records.len(),
        1,
        "{}",
        String::from_utf8_lossy(&older.stdout)
    );
    assert!(records[0]["name"]
        .as_str()
        .unwrap()
        .ends_with("/files/old.parquet"));
}

/// A UTC day as `YYYY-MM-DD`, from milliseconds since the epoch.
fn civil_date(millis: i64) -> String {
    let days = millis / 86_400_000;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}")
}
