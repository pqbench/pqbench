//! Blackbox end-to-end tests of the public `bytemass` command.

use pqbench::bytemass::{aggregate, bytemass, render_json, render_text, BytemassRequest};
use pqbench::viz::{self, MassRecord};

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn request(inputs: Vec<String>) -> BytemassRequest {
    BytemassRequest {
        inputs,
        ..Default::default()
    }
}

#[tokio::test]
async fn measures_a_local_file_from_its_footer() {
    let rows = bytemass(&request(vec![fixture("small_snappy.parquet")]))
        .await
        .unwrap();

    assert!(!rows.is_empty());
    assert!(rows
        .iter()
        .all(|row| row.uri == fixture("small_snappy.parquet")));

    let summary = aggregate(&rows).unwrap();
    assert_eq!(summary.file_count, 1);
    assert!(summary.row_count > 0);
    assert!(!summary.columns.is_empty());
    assert_eq!(rows[0].row_count, summary.row_count);
    assert!(rows[0].size_bytes > 0);

    let text = render_text(&rows).unwrap();
    assert!(text.contains("bytemass: small_snappy.parquet"));
    assert!(text.contains("bytes/row"));
    assert!(text
        .lines()
        .any(|line| line.trim_start().starts_with("total")));
}

#[tokio::test]
async fn emits_composable_json_per_column() {
    let rows = bytemass(&request(vec![fixture("small_snappy.parquet")]))
        .await
        .unwrap();
    let output = render_json(&rows).unwrap();

    let summary: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(summary["file_count"], 1);
    assert!(summary["row_count"].as_u64().unwrap() > 0);
    let columns = summary["columns"].as_array().unwrap();
    assert!(!columns.is_empty());
    assert!(columns[0]["column"].is_string());
    assert!(columns[0]["compressed_bytes"].is_number());
    // The JSON is a flat table, not a d3 treemap hierarchy.
    assert!(summary.get("children").is_none());
}

#[tokio::test]
async fn viz_collects_measured_rows() {
    let rows = bytemass(&request(vec![fixture("small_snappy.parquet")]))
        .await
        .unwrap();
    let records: Vec<MassRecord> = rows
        .into_iter()
        .map(|row| MassRecord {
            id: String::new(),
            file: row.uri,
            size: row.size_bytes,
            row_count: row.row_count,
            column: row.column,
            compressed_bytes: row.compressed_bytes,
            uncompressed_bytes: row.uncompressed_bytes,
            codec: row.codec,
        })
        .collect();
    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    viz::write_report(&prefix, &records, &[]).unwrap();
    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    assert!(html.starts_with("<!DOCTYPE html>"));
    assert!(html.contains("d3-hierarchy@3"));
    assert!(html.contains("small_snappy.parquet"));
}

#[tokio::test]
async fn expands_globs_into_one_collection() {
    let mask = format!("{}/tests/fixtures/*.parquet", env!("CARGO_MANIFEST_DIR"));
    let rows = bytemass(&request(vec![mask])).await.unwrap();

    let summary = aggregate(&rows).unwrap();
    assert!(summary.file_count > 1);
    assert!(render_text(&rows).unwrap().contains("parquet files"));
}

#[tokio::test]
async fn treats_brackets_as_literal_path_characters() {
    let dir = tempfile::tempdir().unwrap();
    let literal = dir.path().join("archive[1].parquet");
    std::fs::copy(fixture("small_snappy.parquet"), &literal).unwrap();
    let rows = bytemass(&request(vec![literal.to_string_lossy().into_owned()]))
        .await
        .unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn rejects_empty_inputs_and_unmatched_masks() {
    assert!(bytemass(&request(vec![])).await.is_err());

    let missing = format!("{}/tests/fixtures/*.missing", env!("CARGO_MANIFEST_DIR"));
    let error = bytemass(&request(vec![missing]))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("mask matched no files"), "{error}");
}

#[cfg(not(feature = "aws"))]
#[tokio::test]
async fn remote_inputs_name_the_missing_aws_feature() {
    let error = bytemass(&request(vec!["s3://bucket/file.parquet".into()]))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("`aws` feature"), "{error}");
}
