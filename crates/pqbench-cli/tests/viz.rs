use std::io::Write;
mod common;

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

fn reported(measured: &std::process::Output) -> std::process::Output {
    pipe(&["summarize", "columns"], &measured.stdout)
}

fn row(table: &str, column: &str, bytes: u64, uncompressed: u64, values: u64) -> serde_json::Value {
    serde_json::json!({
        "table": table,
        "column": column,
        "compressed_bytes": bytes,
        "uncompressed_bytes": uncompressed,
        "value_count": values,
        "bytes_per_value": bytes as f64 / values as f64,
        "compression_ratio": uncompressed as f64 / bytes as f64,
        "file_count": 1,
        "storage_classes": ["STANDARD"],
        "storage_costs": [{
            "storage_class": "STANDARD",
            "dollars_per_gibibyte_per_month": 0.023,
            "cost_per_month": 0.000001,
        }],
    })
}

fn report(html: &str) -> serde_json::Value {
    let start = html
        .find("const REPORT = ")
        .expect("the page embeds its report");
    let body = &html[start + "const REPORT = ".len()..];
    let end = body.find(";\n").expect("the report ends");
    serde_json::from_str(&body[..end]).expect("the report is JSON")
}

#[test]
fn viz_draws_a_summarized_stream_into_html() {
    let measured = common::measured(parquet_fixture());
    assert!(
        measured.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&measured.stderr)
    );
    let summarized = reported(&measured);
    assert!(
        summarized.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&summarized.stderr)
    );

    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    let output = pipe(
        &["viz", "--output", prefix.to_str().unwrap()],
        &summarized.stdout,
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    assert!(html.starts_with("<!DOCTYPE html>"));
    assert!(html.contains("d3-hierarchy@3"));
    assert!(html.contains("Catalogs ("));
    assert!(html.contains("B/row"));
}

#[test]
fn viz_places_a_fileless_row_under_the_local_table() {
    let document = format!(
        "{}\n",
        row(
            "catalogs/local/schemas/local/tables/part-0.parquet",
            "id",
            8,
            16,
            10
        )
    );
    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    let output = pipe(
        &["viz", "--output", prefix.to_str().unwrap()],
        document.as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    assert!(html.contains("\"display_name\":\"local\""), "{html}");
    assert!(
        html.contains("\"display_name\":\"part-0.parquet\""),
        "{html}"
    );
}

#[test]
fn viz_sums_the_rows_of_a_table() {
    let document = format!(
        "{}\n{}\n",
        row("catalogs/shop/schemas/sales/tables/orders", "id", 8, 20, 10),
        row(
            "catalogs/shop/schemas/sales/tables/orders",
            "note",
            2,
            5,
            10
        ),
    );
    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    let output = pipe(
        &["viz", "--output", prefix.to_str().unwrap()],
        document.as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    let report = report(&html);
    let catalog = &report["catalogs"][0];
    let schema = &catalog["schemas"][0];
    let table = &schema["tables"][0];
    assert_eq!(table["compressed_bytes"], 10);
    assert_eq!(table["compression_ratio"], 2.5);
    assert_eq!(table["file_count"], 1);
    assert_eq!(table["columns"][0]["column"], "id");
    assert_eq!(table["columns"][0]["bytes_per_value"], 0.8);
    assert_eq!(catalog["compressed_bytes"], 10);
    assert_eq!(schema["compressed_bytes"], 10);
}

#[test]
fn viz_rejects_a_bytemass_stream() {
    let measured = common::measured(parquet_fixture());
    assert!(measured.status.success());
    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    let output = pipe(&["viz", "-o", prefix.to_str().unwrap()], &measured.stdout);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("summarize columns"), "{stderr}");
}

#[test]
fn viz_requires_output() {
    let measured = common::measured(parquet_fixture());
    assert!(measured.status.success());
    let summarized = reported(&measured);
    assert!(summarized.status.success());
    let output = pipe(&["viz"], &summarized.stdout);
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("--output"), "{stderr}");
}
