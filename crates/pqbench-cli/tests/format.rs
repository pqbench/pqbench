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

fn table(args: &[&str]) -> String {
    let output = pqbench().args(args).output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn lz_table_format_aligns_columns() {
    let stdout = table(&[
        "lz",
        parquet_fixture(),
        "-c",
        "zstd@3",
        "--samples",
        "1",
        "--warmup-iterations",
        "0",
        "--format",
        "table",
    ]);
    assert!(stdout.contains("codec"), "{stdout}");
    assert!(stdout.contains("compress MB/s"), "{stdout}");
    assert!(stdout.contains("ratio"), "{stdout}");
    assert!(stdout.contains("zstd"), "{stdout}");
    assert!(
        !stdout.contains("{\"kind\""),
        "table must not carry NDJSON: {stdout}"
    );
}

#[test]
fn format_json_forces_ndjson() {
    let output = pqbench()
        .args([
            "lz",
            parquet_fixture(),
            "-c",
            "zstd@3",
            "--samples",
            "1",
            "--warmup-iterations",
            "0",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let first = output.stdout.split(|byte| *byte == b'\n').next().unwrap();
    let record: serde_json::Value = serde_json::from_slice(first).expect("ndjson line");
    assert_eq!(record["kind"], "pqbench.lz");
    assert_eq!(record["event"], "begin");
}

#[test]
fn bytemass_table_format_lists_columns() {
    let stdout = table(&["bytemass", parquet_fixture(), "--format", "table"]);
    assert!(stdout.contains("column"), "{stdout}");
    assert!(stdout.contains("codec"), "{stdout}");
    assert!(stdout.contains("url_encoded"), "{stdout}");
    assert!(
        !stdout.contains("{\"kind\""),
        "table must not carry NDJSON: {stdout}"
    );
}

#[test]
fn profile_table_format_lists_columns() {
    let stdout = table(&[
        "profile",
        parquet_fixture(),
        "--rows",
        "first:64",
        "--format",
        "table",
    ]);
    assert!(stdout.contains("column"), "{stdout}");
    assert!(stdout.contains("ndv"), "{stdout}");
    assert!(stdout.contains("url_encoded"), "{stdout}");
    assert!(
        !stdout.contains("{\"kind\""),
        "table must not carry NDJSON: {stdout}"
    );
}

#[test]
fn experiment_table_format_lists_trials() {
    let stdout = table(&[
        "experiment",
        parquet_fixture(),
        "--rows",
        "first:64",
        "--format",
        "table",
    ]);
    assert!(stdout.contains("trial"), "{stdout}");
    assert!(stdout.contains("bytes/row"), "{stdout}");
    assert!(stdout.contains("control"), "{stdout}");
    assert!(
        !stdout.contains("{\"kind\""),
        "table must not carry NDJSON: {stdout}"
    );
}

#[test]
fn table_table_format_names_each_file() {
    let size = std::fs::metadata(parquet_fixture()).unwrap().len();
    let document = format!(
        "{{\"kind\":\"pqbench.table\",\"version\":1,\"format\":\"delta\",\"uri\":\"/tmp/table\",\
         \"snapshot_version\":0,\"partition_columns\":[],\"log\":[],\
         \"files\":[{{\"path\":\"small_reddit_none.parquet\",\"uri\":\"{}\",\
         \"size_bytes\":{size},\"stats\":{{\"num_records\":3000}}}}]}}\n",
        parquet_fixture()
    );
    let output = pipe(&["table", "--format", "table"], &document);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("path"), "{stdout}");
    assert!(stdout.contains("small_reddit_none.parquet"), "{stdout}");
    assert!(stdout.contains("3000"), "{stdout}");
    assert!(
        !stdout.contains("{\"kind\""),
        "table must not carry NDJSON: {stdout}"
    );
}
