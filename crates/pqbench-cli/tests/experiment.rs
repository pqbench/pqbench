use std::io::Write;
use std::process::{Command, Stdio};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

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

fn pipe(args: &[&str], stdin: &[u8]) -> std::process::Output {
    let mut child = pqbench()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn experiment_streams_a_control_trial() {
    let output = pqbench()
        .args(["experiment", parquet_fixture(), "--rows", "first:32"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records[0]["kind"], "pqbench.experiment");
    assert_eq!(records[0]["event"], "begin");
    assert_eq!(records[0]["aim"], "storage");
    assert!(records.iter().any(|record| {
        record["kind"] == "pqbench.experiment-trial" && record["name"] == "control"
    }));
    assert!(records
        .iter()
        .any(|record| record["kind"] == "pqbench.experiment-column"
            && record["compressed_bytes"].as_u64().is_some()));
}

#[test]
fn rewrite_and_aim_are_requestable() {
    let output = pqbench()
        .args([
            "experiment",
            parquet_fixture(),
            "--rows",
            "first:32",
            "--rewrite",
            "sort:label",
            "--aim",
            "skipping",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records[0]["aim"], "skipping");
    assert!(records.iter().any(|record| {
        record["kind"] == "pqbench.experiment-trial" && record["name"] == "sort:label"
    }));
    assert!(records.iter().any(|record| {
        record["kind"] == "pqbench.experiment-column"
            && record["trial"] == "sort:label"
            && (record["skip_span_ratio"].as_f64().is_some()
                || record["skip_point_equal_fraction"].as_f64().is_some())
    }));
}

#[test]
fn dump_pipe_feeds_experiment() {
    let dumped = pqbench()
        .args(["dump", parquet_fixture(), "--row-groups", "first:1"])
        .output()
        .unwrap();
    assert!(
        dumped.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&dumped.stderr)
    );
    let experimented = pipe(
        &[
            "experiment",
            "--rows",
            "first:16",
            "--rewrite",
            "codec:snappy",
        ],
        &dumped.stdout,
    );
    assert!(
        experimented.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&experimented.stderr)
    );
    let records = ndjson_records(&experimented.stdout);
    assert!(records
        .iter()
        .any(|record| record["kind"] == "pqbench.experiment-trial"
            && record["name"] == "codec:snappy"));
}

#[test]
fn unknown_rewrite_fails() {
    let output = pqbench()
        .args([
            "experiment",
            parquet_fixture(),
            "--rows",
            "first:8",
            "--rewrite",
            "correlate:text",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("rewrite"), "{stderr}");
}
