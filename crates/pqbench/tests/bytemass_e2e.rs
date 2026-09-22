//! Blackbox end-to-end tests of the public `bytemass` command.

use pqbench::bytemass::{
    aggregate, bytemass, render_html, render_json, render_text, BytemassRequest,
};

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
        .all(|row| row.file == fixture("small_snappy.parquet")));

    let summary = aggregate(&rows).unwrap();
    assert_eq!(summary.file_count, 1);
    assert!(summary.num_rows > 0);
    assert!(!summary.columns.is_empty());
    assert_eq!(rows[0].num_rows, summary.num_rows);
    assert!(rows[0].size > 0);

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
    assert!(summary["num_rows"].as_u64().unwrap() > 0);
    let columns = summary["columns"].as_array().unwrap();
    assert!(!columns.is_empty());
    assert!(columns[0]["path"].is_string());
    assert!(columns[0]["compressed_bytes"].is_number());
    // The JSON is a flat table, not a d3 treemap hierarchy.
    assert!(summary.get("children").is_none());
}

#[tokio::test]
async fn emits_a_self_contained_d3_page() {
    let rows = bytemass(&request(vec![fixture("small_snappy.parquet")]))
        .await
        .unwrap();
    let page = render_html(&rows).unwrap();

    assert!(page.starts_with("<!DOCTYPE html>"));
    assert!(page.contains("<title>small_snappy.parquet</title>"));
    assert!(page.contains("d3-hierarchy@3"));
    assert!(page.contains("\"name\": \"small_snappy.parquet\""));
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
