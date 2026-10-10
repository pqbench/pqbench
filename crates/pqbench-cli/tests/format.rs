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
    assert_eq!(record["kind"], "pqbench.lz-row");
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
