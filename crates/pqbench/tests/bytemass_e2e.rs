//! Blackbox end-to-end tests of the public `bytemass` command.

mod common;

use pqbench::bytemass::{bytemass, measure_files, BytemassRequest};
use pqbench::v1;

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
    let path = fixture("small_snappy.parquet");
    let columns = bytemass(common::lake(), &request(vec![path.clone()]))
        .await
        .unwrap();

    assert!(!columns.is_empty());
    for column in &columns {
        assert!(
            column.compressed_bytes.unwrap_or(0) > 0,
            "{} has no on-disk bytes",
            column.name
        );
        assert!(
            column.value_count.unwrap_or(0) > 0,
            "{} has no values",
            column.name
        );
        let (blob, _) = v1::name::parse_bytemass(&column.name).expect("a bytemass name");
        assert_eq!(blob, v1::name::blob_name(&path));
    }
}

#[tokio::test]
async fn treats_brackets_as_literal_path_characters() {
    let directory = tempfile::tempdir().unwrap();
    let literal = directory.path().join("archive[1].parquet");
    std::fs::copy(fixture("small_snappy.parquet"), &literal).unwrap();
    let columns = bytemass(
        common::lake(),
        &request(vec![literal.to_string_lossy().into_owned()]),
    )
    .await
    .unwrap();
    assert!(!columns.is_empty());
}

#[tokio::test]
async fn rejects_empty_inputs() {
    assert!(bytemass(common::lake(), &request(vec![])).await.is_err());
}

#[tokio::test]
async fn a_pattern_is_not_expanded() {
    let missing = format!("{}/tests/fixtures/*.missing", env!("CARGO_MANIFEST_DIR"));
    let error = bytemass(common::lake(), &request(vec![missing]))
        .await
        .unwrap_err()
        .to_string();
    assert!(!error.contains("mask matched no files"), "{error}");
}

#[tokio::test]
async fn viz_draws_measured_columns() {
    let files = measure_files(
        common::lake(),
        &request(vec![fixture("small_snappy.parquet")]),
    )
    .await
    .unwrap();
    let blobs: Vec<_> = files.iter().map(|file| file.blob.clone()).collect();
    let columns: Vec<_> = files.into_iter().flat_map(|file| file.columns).collect();

    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    pqbench::viz::write_report(&prefix, &columns, &blobs).unwrap();
    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    assert!(html.starts_with("<!DOCTYPE html>"));
    assert!(html.contains("d3-hierarchy@3"));
    assert!(html.contains("small_snappy.parquet"));
}

#[cfg(not(feature = "aws"))]
#[tokio::test]
async fn remote_inputs_name_the_missing_aws_feature() {
    let error = bytemass(
        common::lake(),
        &request(vec!["s3://bucket/file.parquet".into()]),
    )
    .await
    .unwrap_err();
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    assert!(message.contains("`aws` feature"), "{message}");
}
