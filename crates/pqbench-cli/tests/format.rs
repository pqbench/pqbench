mod common;

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
    format_stream(&output.stdout)
}

/// `pqbench format` over a record stream.
fn format_stream(records: &[u8]) -> String {
    use std::io::Write as _;
    use std::process::Stdio;

    let mut child = pqbench()
        .arg("format")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(records).unwrap();
    let formatted = child.wait_with_output().unwrap();
    assert!(
        formatted.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&formatted.stderr)
    );
    String::from_utf8(formatted.stdout).unwrap()
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
    assert!(
        record["name"].as_str().unwrap().contains("/codecs/"),
        "{record}"
    );
}

#[test]
fn bytemass_table_format_lists_columns() {
    let measured = common::measured(parquet_fixture());
    assert!(measured.status.success());
    let stdout = format_stream(&measured.stdout);
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
    let stdout = table(&["profile", parquet_fixture(), "--rows", "first:64"]);
    assert!(stdout.contains("column"), "{stdout}");
    assert!(stdout.contains("ndv"), "{stdout}");
    assert!(stdout.contains("url_encoded"), "{stdout}");
    assert!(
        !stdout.contains("{\"kind\""),
        "table must not carry NDJSON: {stdout}"
    );
}

#[test]
fn mass_rows_render_as_a_column_mass_table() {
    let document = concat!(
        r#"{"table":"catalogs/shop/schemas/sales/tables/orders","column":"id","compressed_bytes":8,"value_count":10,"bytes_per_value":0.8,"file_count":1,"storage_classes":["STANDARD"],"storage_costs":[{"storage_class":"STANDARD","dollars_per_gibibyte_per_month":0.023,"cost_per_month":0.0045}]}"#,
        "\n",
        r#"{"table":"catalogs/shop/schemas/sales/tables/orders","column":"note","compressed_bytes":2,"value_count":10,"bytes_per_value":0.2,"file_count":1}"#,
        "\n",
        r#"{"column":"id","left_bytes":8,"right_bytes":4}"#,
        "\n",
    );
    let stdout = format_stream(document.as_bytes());
    assert!(stdout.contains("bytes/value"), "{stdout}");
    assert!(stdout.contains("sales.orders"), "{stdout}");
    assert!(stdout.contains("$0.0045"), "{stdout}");
    assert!(
        !stdout.contains("left_bytes"),
        "a nameless delta is not table data: {stdout}"
    );
}
