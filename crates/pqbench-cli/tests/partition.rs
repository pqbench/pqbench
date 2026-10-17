use std::io::Write;
use std::process::{Command, Stdio};

#[cfg(feature = "delta")]
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

#[cfg(feature = "delta")]
fn ndjson_records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

#[cfg(feature = "delta")]
fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

#[cfg(feature = "delta")]
struct DeltaFixture {
    // Held only to keep the temporary directory alive for the test's duration.
    // aipnaming: allow(aip-140/underscores)
    _directory: tempfile::TempDir,
    path: std::path::PathBuf,
}

#[cfg(feature = "delta")]
fn delta_fixture() -> DeltaFixture {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_path_buf();
    std::fs::create_dir(root.join("_delta_log")).unwrap();
    let data = root.join("data.parquet");
    std::fs::copy(parquet_fixture(), &data).unwrap();
    let size = std::fs::metadata(&data).unwrap().len();
    let commit = json!([
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {
            "id": "11111111-1111-1111-1111-111111111111",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0
        }},
        {"add": {
            "path": "data.parquet",
            "partitionValues": {},
            "size": size,
            "modificationTime": 0,
            "dataChange": true,
            "stats": "{\"numRecords\":3000,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":1},\"nullCount\":{\"id\":0},\"tightBounds\":true}"
        }}
    ]);
    let text = commit
        .as_array()
        .unwrap()
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

/// A Delta table whose single commit records `commitInfo.timestamp` (ms).
#[cfg(feature = "delta")]
fn dated_delta_fixture(timestamp: i64) -> DeltaFixture {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_path_buf();
    std::fs::create_dir(root.join("_delta_log")).unwrap();
    let data = root.join("data.parquet");
    std::fs::copy(parquet_fixture(), &data).unwrap();
    let size = std::fs::metadata(&data).unwrap().len();
    let commit = json!([
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {
            "id": "11111111-1111-1111-1111-111111111111",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 0
        }},
        {"add": {
            "path": "data.parquet",
            "partitionValues": {},
            "size": size,
            "modificationTime": 0,
            "dataChange": true,
            "stats": "{\"numRecords\":3000,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":1},\"nullCount\":{\"id\":0},\"tightBounds\":true}"
        }},
        {"commitInfo": {"timestamp": timestamp, "operation": "WRITE"}}
    ]);
    let text = commit
        .as_array()
        .unwrap()
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

/// `list partitions` dates each commit by its `commitInfo.timestamp` and groups it
/// into an epoch-aligned, half-open UTC window.
#[cfg(feature = "delta")]
#[test]
fn partition_ls_groups_commits_into_natural_windows() {
    let timestamp = 1_700_000_000_000i64;
    let fixture = dated_delta_fixture(timestamp);
    let output = pipe(
        &[
            "list",
            "partitions",
            fixture.path.to_str().unwrap(),
            "--format",
            "json",
        ],
        "",
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records.len(), 1);
    let partition = &records[0];
    let day = 86_400_000i64;
    let first = timestamp.div_euclid(day) * day;
    assert_eq!(partition["start_time"], first);
    assert_eq!(partition["end_time"], first + day);
    assert_eq!(partition["commit_versions"][0], 0);
    assert_eq!(partition["commit_infos"][0]["operation"], "WRITE");
    assert_eq!(partition["commit_infos"][0]["timestamp"], timestamp);
    assert!(partition.get("kind").is_none(), "{partition}");
}

/// `--every` sets the window width; the boundary stays epoch-aligned.
#[cfg(feature = "delta")]
#[test]
fn partition_ls_honors_the_window_width() {
    let timestamp = 1_700_000_000_000i64;
    let fixture = dated_delta_fixture(timestamp);
    let output = pipe(
        &[
            "list",
            "partitions",
            fixture.path.to_str().unwrap(),
            "--every",
            "1h",
            "--format",
            "json",
        ],
        "",
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records.len(), 1);
    let hour = 3_600_000i64;
    let first = timestamp.div_euclid(hour) * hour;
    assert_eq!(records[0]["start_time"], first);
    assert_eq!(records[0]["end_time"], first + hour);
}

/// A commit the log does not date is omitted: a window never claims a commit
/// it cannot place.
#[cfg(feature = "delta")]
#[test]
fn partition_ls_skips_commits_without_a_time() {
    let fixture = delta_fixture();
    let output = pipe(
        &[
            "list",
            "partitions",
            fixture.path.to_str().unwrap(),
            "--format",
            "json",
        ],
        "",
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(ndjson_records(&output.stdout).is_empty());
}

/// A malformed `--every` fails loudly before any table is read.
#[cfg(feature = "delta")]
#[test]
fn partition_ls_rejects_a_bad_window() {
    let fixture = dated_delta_fixture(0);
    let output = pipe(
        &[
            "list",
            "partitions",
            fixture.path.to_str().unwrap(),
            "--every",
            "1x",
        ],
        "",
    );
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--every unit"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "delta")]
#[test]
fn partition_ls_exits_cleanly_when_stdout_is_closed() {
    let fixture = dated_delta_fixture(1_700_000_000_000);
    let mut child = pqbench()
        .args(["list", "partitions", fixture.path.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdout.take());
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A table ref from a catalog walk must carry a storage URI; one without it
/// fails loudly and names the stage that fills it.
#[test]
fn partition_ls_rejects_a_ref_without_a_storage_path() {
    let input = r#"{"name":"catalogs/c/schemas/s/tables/t"}"#;
    let output = pipe(&["list", "partitions"], input);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("storage path"), "{stderr}");
}
