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
fn profile_streams_column_facts() {
    let output = pqbench()
        .args(["profile", parquet_fixture(), "--rows", "first:32"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(records[0]["kind"], "pqbench.profile");
    assert_eq!(records[0]["event"], "begin");
    assert!(records[0]["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .any(|capability| capability["name"] == "dependencies"
            && capability["flag"] == "--dependencies"));
    let columns: Vec<_> = records
        .iter()
        .filter(|record| record["kind"] == "pqbench.profile-column")
        .collect();
    assert!(columns.iter().any(|record| record["column"] == "text"
        && record["physical_kind"] == "STRING"
        && record["ascii_fraction"].as_f64().is_some()
        && record["length_mean"].as_f64().is_some()));
    assert!(columns
        .iter()
        .all(|record| record["ndv"].as_u64().unwrap() <= record["num_values"].as_u64().unwrap()));
    assert!(records
        .iter()
        .all(|record| record["kind"] != "pqbench.profile-dependency"));
    let end = records.last().unwrap();
    assert_eq!(end["event"], "end");
    assert_eq!(end["dependency_count"], 0);
}

#[test]
fn dump_pipe_feeds_profile() {
    let dumped = pqbench()
        .args(["dump", parquet_fixture(), "--row-groups", "first:1"])
        .output()
        .unwrap();
    assert!(
        dumped.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&dumped.stderr)
    );
    assert!(dumped.stdout.starts_with(b"PAR1"));
    let profiled = pipe(&["profile", "--rows", "first:16"], &dumped.stdout);
    assert!(
        profiled.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&profiled.stderr)
    );
    let records = ndjson_records(&profiled.stdout);
    assert!(records
        .iter()
        .any(|record| record["kind"] == "pqbench.profile-column"));
}

#[test]
fn dependencies_are_opt_in() {
    let output = pqbench()
        .args([
            "profile",
            parquet_fixture(),
            "--rows",
            "first:16",
            "--columns",
            "text",
            "--columns",
            "label",
            "--dependencies",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert_eq!(
        records
            .iter()
            .filter(|record| record["kind"] == "pqbench.profile-column")
            .count(),
        2
    );
    assert!(records.iter().any(|record| {
        record["kind"] == "pqbench.profile-dependency"
            && ((record["left"] == "label" && record["right"] == "text")
                || (record["left"] == "text" && record["right"] == "label"))
    }));
}

#[test]
fn profile_rejects_a_table_document() {
    let output = pipe(
        &["profile"],
        br#"{"kind":"pqbench.table","version":1}
"#,
    );
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("dump"), "{stderr}");
}
