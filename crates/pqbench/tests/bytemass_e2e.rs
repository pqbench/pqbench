//! Blackbox end-to-end tests of the public `list columns` command.

mod common;

use pqbench::bytemass::list_columns;
use pqbench::v1;

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

async fn measure(path: &str) -> Vec<v1::Column> {
    list_columns(common::lake(), &v1::name::blob_name(path))
        .await
        .unwrap()
}

#[tokio::test]
async fn measures_a_local_file_from_its_footer() {
    let path = fixture("small_snappy.parquet");
    let columns = measure(&path).await;

    assert!(!columns.is_empty());
    for column in &columns {
        let mass = column.mass.as_ref().expect("a measured column");
        assert!(
            mass.compressed_bytes.unwrap_or(0) > 0,
            "{} has no on-disk bytes",
            column.name
        );
        assert!(
            mass.value_count.unwrap_or(0) > 0,
            "{} has no values",
            column.name
        );
        let (service, locator, _) = v1::Column::parse_name(&column.name).expect("a column name");
        assert_eq!(
            v1::name::blob(&service, &locator),
            v1::name::blob_name(&path)
        );
    }
}

#[tokio::test]
async fn reports_the_dictionary_page_and_data_encoding() {
    let columns = measure(&fixture("small_snappy.parquet")).await;
    let column = &columns[0];
    let mass = column.mass.as_ref().expect("a measured column");

    assert_eq!(column.physical_data_type.as_deref(), Some("BYTE_ARRAY"));
    assert_eq!(
        mass.data_encodings,
        Some(vec!["RLE_DICTIONARY".to_string()])
    );
    let dictionary_bytes = mass.dictionary_bytes.expect("a dictionary page");
    let compressed_bytes = mass.compressed_bytes.expect("compressed bytes");
    assert!(dictionary_bytes > 0);
    assert!(dictionary_bytes < compressed_bytes);
    assert!(mass.unencoded_bytes.expect("unencoded bytes") > mass.uncompressed_bytes.unwrap_or(0));
    assert!(mass.bytes_per_row.expect("bytes per row") > 0.0);
    assert!(mass.dictionary_fraction.expect("dictionary fraction") > 0.0);
}

#[tokio::test]
async fn treats_brackets_as_literal_path_characters() {
    let directory = tempfile::tempdir().unwrap();
    let literal = directory.path().join("archive[1].parquet");
    std::fs::copy(fixture("small_snappy.parquet"), &literal).unwrap();
    assert!(!measure(&literal.to_string_lossy()).await.is_empty());
}

#[tokio::test]
async fn a_pattern_is_not_expanded() {
    let missing = format!("{}/tests/fixtures/*.missing", env!("CARGO_MANIFEST_DIR"));
    let error = list_columns(common::lake(), &v1::name::blob_name(&missing))
        .await
        .unwrap_err()
        .to_string();
    assert!(!error.contains("mask matched no files"), "{error}");
}

#[tokio::test]
async fn viz_draws_summarized_columns() {
    let path = fixture("small_snappy.parquet");
    let blob_name = v1::name::blob_name(&path);
    let blob = pqbench::blob::get(common::lake(), &blob_name)
        .await
        .unwrap();
    let mut columns = measure(&path).await;
    for column in &mut columns {
        column.table = Some(v1::name::local_table(&path));
    }
    let mut summary = pqbench::mass::ColumnSummary::new();
    summary.add_blob(&blob).unwrap();
    for column in &columns {
        summary.add_column(column).unwrap();
    }
    let rows = summary.into_rows().unwrap();

    let directory = tempfile::tempdir().unwrap();
    let prefix = directory.path().join("report");
    pqbench::viz::write_report(&prefix, &rows).unwrap();
    let html = std::fs::read_to_string(prefix.with_extension("html")).unwrap();
    assert!(html.starts_with("<!DOCTYPE html>"));
    assert!(html.contains("d3-hierarchy@3"));
    assert!(html.contains("small_snappy.parquet"));
}

#[cfg(not(feature = "aws"))]
#[tokio::test]
async fn remote_inputs_name_the_missing_aws_feature() {
    let error = list_columns(
        common::lake(),
        &v1::name::blob_name("s3://bucket/file.parquet"),
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
