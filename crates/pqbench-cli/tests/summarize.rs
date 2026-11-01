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

fn read_rows(output: &std::process::Output) -> Vec<serde_json::Value> {
    let text = String::from_utf8(output.stdout.clone()).unwrap();
    text.lines()
        .map(|line| serde_json::from_str(line).expect("a JSON row"))
        .collect()
}

fn measured_column(
    service: &str,
    locator: &str,
    table: &str,
    sample_rate: f64,
    compressed_bytes: u64,
) -> String {
    let blob = pqbench::v1::name::blob_name(&format!("{service}://{locator}"));
    let column = format!("{blob}/columns/id");
    let file = serde_json::json!({
        "name": blob,
        "size_bytes": 40,
        "storage_class": "STANDARD",
        "sample_rate": sample_rate,
    });
    let row = serde_json::json!({
        "name": column,
        "table": table,
        "sample_rate": sample_rate,
        "mass": {
            "compressed_bytes": compressed_bytes,
            "uncompressed_bytes": compressed_bytes * 2,
            "value_count": 10,
        },
    });
    format!("{file}\n{row}\n")
}

#[test]
fn summarize_columns_writes_one_row_per_table_column() {
    let measured = common::measured(parquet_fixture());
    assert!(
        measured.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&measured.stderr)
    );
    let output = pipe(&["summarize", "columns"], &measured.stdout);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let rows = read_rows(&output);
    assert!(!rows.is_empty());
    for row in &rows {
        assert!(row["table"].as_str().unwrap().starts_with("catalogs/"));
        assert!(row["column"].is_string(), "{row}");
        assert!(row["compressed_bytes"].as_u64().unwrap() > 0, "{row}");
        assert!(row["compression_ratio"].as_f64().unwrap() >= 1.0, "{row}");
    }
}

#[test]
fn summarize_columns_applies_the_sample_weight() {
    let document = measured_column(
        "s3",
        "lake/orders/part-0.parquet",
        "catalogs/shop/schemas/sales/tables/orders",
        0.5,
        8,
    );
    let output = pipe(&["summarize", "columns"], document.as_bytes());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let rows = read_rows(&output);
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["compressed_bytes"], 16);
    assert_eq!(row["uncompressed_bytes"], 32);
    assert_eq!(row["value_count"], 20);
    assert_eq!(row["bytes_per_value"], 0.8);
    assert_eq!(row["file_count"], 2);
    assert_eq!(row["storage_classes"], serde_json::json!(["STANDARD"]));

    // 40 bytes kept at 0.5 stands for 80 bytes of storage.
    let cost = &row["storage_costs"][0];
    assert_eq!(cost["dollars_per_gibibyte_per_month"], 0.023);
    let expected = 80.0 / 1_073_741_824.0 * 0.023;
    assert!((cost["cost_per_month"].as_f64().unwrap() - expected).abs() < 1e-18);
}

#[test]
fn summarize_columns_carries_every_intelligent_tiering_sub_tier() {
    let document = measured_column(
        "s3",
        "lake/orders/part-0.parquet",
        "catalogs/shop/schemas/sales/tables/orders",
        1.0,
        8,
    )
    .replace("STANDARD", "INTELLIGENT_TIERING");
    let output = pipe(&["summarize", "columns"], document.as_bytes());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let rows = read_rows(&output);
    let tiers = rows[0]["storage_costs"].as_array().unwrap();
    assert_eq!(tiers.len(), 5, "{tiers:?}");
    assert_eq!(tiers[0]["access_tier"], "FREQUENT_ACCESS");
    assert_eq!(tiers[4]["access_tier"], "DEEP_ARCHIVE_ACCESS");
    assert_eq!(tiers[4]["dollars_per_gibibyte_per_month"], 0.00099);
}

#[test]
fn summarize_columns_rejects_a_table_file() {
    let document = r#"{"name":"catalogs/local/schemas/local/tables/t/files/p","blob":"blobs/file/%2Ftmp%2Ft"}"#;
    let output = pipe(&["summarize", "columns"], document.as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("list columns"), "{stderr}");
}

#[test]
fn summarize_columns_rejects_a_sample_rate_outside_the_unit_interval() {
    let document = measured_column(
        "s3",
        "lake/orders/part-0.parquet",
        "catalogs/shop/schemas/sales/tables/orders",
        2.0,
        8,
    );
    let output = pipe(&["summarize", "columns"], document.as_bytes());
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("sample_rate"), "{stderr}");
}
