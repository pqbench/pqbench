use std::process::Command;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

fn ndjson(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

#[test]
fn profile_streams_column_facts_for_a_sample() {
    let output = pqbench()
        .args(["profile", parquet_fixture()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records[0]["kind"], "pqbench.profile");
    assert_eq!(records[0]["event"], "begin");
    let columns: Vec<&serde_json::Value> = records
        .iter()
        .filter(|record| record["kind"] == "pqbench.profile-column")
        .collect();
    assert!(!columns.is_empty(), "{records:?}");
    let column = columns[0];
    assert!(column["id"].is_string());
    assert!(column["column"].is_string());
    assert!(column["null_fraction"].is_number());
    assert!(column["ndv"].is_number());
    assert_eq!(records.last().unwrap()["event"], "end");
    assert_eq!(records.last().unwrap()["column_count"], columns.len());
}

#[test]
fn profile_honours_columns_and_rows() {
    let output = pqbench()
        .args([
            "profile",
            parquet_fixture(),
            "--rows",
            "first:32",
            "--columns",
            "text",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let columns: Vec<&serde_json::Value> = records
        .iter()
        .filter(|record| record["kind"] == "pqbench.profile-column")
        .collect();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["column"], "text");
    assert_eq!(records.last().unwrap()["row_count"], 32);
}

#[test]
fn profile_rejects_a_bad_rows_method() {
    let output = pqbench()
        .args(["profile", parquet_fixture(), "--rows", "last:5"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--rows"), "{stderr}");
}
