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

#[test]
fn measures_a_local_file_from_its_footer() {
    let output = bytemass(&request(vec![fixture("small_snappy.parquet")])).unwrap();

    let summary = output.summary.as_ref().unwrap();
    assert_eq!(summary.file_count, 1);
    assert!(summary.num_rows > 0);
    assert!(!summary.columns.is_empty());

    let files = output.files.as_ref().unwrap();
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

#[test]
fn emits_the_tree_as_composable_json() {
    let mut request = request(vec![fixture("small_snappy.parquet")]);
    request.is_json = Some(true);
    let output = bytemass(&request).unwrap().to_string();

    let tree: serde_json::Value = serde_json::from_str(&output).unwrap();
    assert_eq!(tree["name"], "small_snappy.parquet");
    assert!(tree["value"].is_number());
    assert!(tree["children"].is_array());
}

#[test]
fn emits_a_self_contained_d3_page() {
    let mut request = request(vec![fixture("small_snappy.parquet")]);
    request.is_d3 = Some(true);
    let page = bytemass(&request).unwrap().to_string();

    assert!(page.starts_with("<!DOCTYPE html>"));
    assert!(page.contains("<title>small_snappy.parquet</title>"));
    assert!(page.contains("d3-hierarchy@3"));
    assert!(page.contains("\"name\": \"small_snappy.parquet\""));
}

#[test]
fn expands_globs_into_one_labelled_collection() {
    let mask = format!("{}/tests/fixtures/*.parquet", env!("CARGO_MANIFEST_DIR"));
    let output = bytemass(&request(vec![mask])).unwrap();

    let summary = output.summary.as_ref().unwrap();
    assert!(summary.file_count > 1);
    assert_eq!(output.files.as_ref().unwrap().len(), summary.file_count);
    assert!(output.to_string().contains("parquet files"));
}

#[test]
fn rejects_empty_masks_and_conflicting_formats() {
    let missing = format!("{}/tests/fixtures/*.missing", env!("CARGO_MANIFEST_DIR"));
    let error = bytemass(&request(vec![missing])).unwrap_err().to_string();
    assert!(error.contains("mask matched no files"), "{error}");

    let mut conflict = request(vec![fixture("small_snappy.parquet")]);
    conflict.is_json = Some(true);
    conflict.is_d3 = Some(true);
    assert!(bytemass(&conflict).is_err());
}

#[cfg(not(feature = "aws"))]
#[test]
fn remote_inputs_name_the_missing_aws_feature() {
    let error = bytemass(&request(vec!["s3://bucket/file.parquet".into()]))
        .unwrap_err()
        .to_string();
    assert!(error.contains("`aws` feature"), "{error}");
}
