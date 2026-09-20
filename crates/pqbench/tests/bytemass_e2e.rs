//! Blackbox end-to-end tests of the public `bytemass` command.

use pqbench::bytemass::{bytemass, BytemassRequest};

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

fn request(inputs: Vec<String>) -> BytemassRequest {
    BytemassRequest {
        inputs,
        ..BytemassRequest::default()
    }
}

#[tokio::test]
async fn measures_a_local_file_from_its_footer() {
    let output = bytemass(&request(vec![fixture("small_snappy.parquet")]))
        .await
        .unwrap();

    let summary = &output.summary;
    assert_eq!(summary.file_count, 1);
    assert!(summary.num_rows > 0);
    assert!(!summary.columns.is_empty());

    let files = &output.files;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, fixture("small_snappy.parquet"));
    assert_eq!(files[0].mass.num_rows, summary.num_rows);

    let text = output.to_string();
    assert!(text.contains("bytemass: small_snappy.parquet"));
    assert!(text.contains("bytes/row"));
    assert!(text
        .lines()
        .any(|line| line.trim_start().starts_with("total")));
}

#[tokio::test]
async fn emits_the_tree_as_composable_json() {
    let mut request = request(vec![fixture("small_snappy.parquet")]);
    request.is_json = Some(true);
    let output = bytemass(&request).await.unwrap().to_string();

    let tree: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(tree["name"], "small_snappy.parquet");
    assert!(tree["value"].is_number());
    assert!(tree["children"].is_array());
}

#[tokio::test]
async fn emits_a_self_contained_d3_page() {
    let mut request = request(vec![fixture("small_snappy.parquet")]);
    request.is_d3 = Some(true);
    let page = bytemass(&request).await.unwrap().to_string();

    assert!(page.starts_with("<!DOCTYPE html>"));
    assert!(page.contains("<title>small_snappy.parquet</title>"));
    assert!(page.contains("d3-hierarchy@3"));
    assert!(page.contains("\"name\": \"small_snappy.parquet\""));
}

#[tokio::test]
async fn expands_globs_into_one_labelled_collection() {
    let mask = format!("{}/tests/fixtures/*.parquet", env!("CARGO_MANIFEST_DIR"));
    let output = bytemass(&request(vec![mask])).await.unwrap();

    let summary = &output.summary;
    assert!(summary.file_count > 1);
    assert_eq!(output.files.len(), summary.file_count);
    assert!(output.to_string().contains("parquet files"));
}

#[tokio::test]
async fn treats_brackets_as_literal_path_characters() {
    let dir = tempfile::tempdir().unwrap();
    let literal = dir.path().join("archive[1].parquet");
    std::fs::copy(fixture("small_snappy.parquet"), &literal).unwrap();
    let output = bytemass(&request(vec![literal.to_string_lossy().into_owned()]))
        .await
        .unwrap();
    assert_eq!(output.files.len(), 1);
}

#[tokio::test]
async fn rejects_empty_inputs_empty_masks_and_conflicting_formats() {
    assert!(bytemass(&request(vec![])).await.is_err());

    let missing = format!("{}/tests/fixtures/*.missing", env!("CARGO_MANIFEST_DIR"));
    let error = bytemass(&request(vec![missing]))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("mask matched no files"), "{error}");

    let mut conflict = request(vec![fixture("small_snappy.parquet")]);
    conflict.is_json = Some(true);
    conflict.is_d3 = Some(true);
    assert!(bytemass(&conflict).await.is_err());
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
