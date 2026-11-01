//! Read a blob's footer and fold its column chunks into per-column masses.

use std::collections::BTreeMap;
use std::path::Path;

use crate::third_party::parquet::api::{read_file_masses, ColumnMass, FileMass};
use crate::v1;
use crate::Lake;

use super::remote;
use super::Error;

/// List the blob's columns, each with its byte mass.
pub(super) async fn list_columns(lake: &Lake, name: &str) -> Result<Vec<v1::Column>, Error> {
    let uri = v1::name::blob_uri(name)
        .ok_or_else(|| Error::Name(format!("{name} is not a blobs/… name")))?;
    let mass = read_input(lake, &uri).await?;
    columns(name, mass)
}

/// Fold one `ColumnMass` per chunk into one [`v1::Column`] per leaf column.
fn columns(blob_name: &str, mass: FileMass) -> Result<Vec<v1::Column>, Error> {
    let (service, locator) = v1::name::parse_blob(blob_name)
        .ok_or_else(|| Error::Name(format!("{blob_name} is not a blobs/… name")))?;
    let row_count = mass.row_count;
    let row_group_count = mass.row_group_count as u64;
    let mut at: BTreeMap<String, usize> = BTreeMap::new();
    let mut columns: Vec<v1::Column> = Vec::new();
    let mut chunks: Vec<Vec<ColumnMass>> = Vec::new();
    for chunk in mass.columns {
        let index = *at.entry(chunk.column.clone()).or_insert_with(|| {
            let position = columns.len() as u64;
            columns.push(v1::Column {
                name: v1::name::column(&service, &locator, &chunk.column),
                position: Some(position),
                physical_data_type: Some(chunk.physical_data_type.clone()),
                max_definition_level: Some(chunk.max_definition_level),
                max_repetition_level: Some(chunk.max_repetition_level),
                row_group_count: Some(row_group_count),
                ..v1::Column::default()
            });
            chunks.push(Vec::new());
            columns.len() - 1
        });
        chunks[index].push(chunk);
    }
    for (column, chunks) in columns.iter_mut().zip(chunks) {
        column.mass = Some(measure(&chunks, row_count));
    }
    Ok(columns)
}

fn measure(chunks: &[ColumnMass], row_count: u64) -> v1::Bytemass {
    let compressed_bytes = chunks.iter().map(|chunk| chunk.compressed_bytes).sum();
    let uncompressed_bytes = chunks.iter().map(|chunk| chunk.uncompressed_bytes).sum();
    let value_count = chunks.iter().map(|chunk| chunk.num_values).sum();
    let dictionary = chunks.iter().any(|chunk| chunk.dictionary);
    let encodings = union(chunks.iter().map(|chunk| chunk.encodings.as_slice()));
    let data_encodings = known_union(chunks.iter().map(|chunk| chunk.data_encodings.as_deref()));
    let null_count = known_sum(chunks.iter().map(|chunk| chunk.null_count));
    let dictionary_bytes = known_sum(
        chunks
            .iter()
            .filter(|chunk| chunk.dictionary)
            .map(|chunk| chunk.dictionary_bytes),
    );
    let unencoded_bytes = known_sum(chunks.iter().map(|chunk| chunk.unencoded_bytes));
    let (distinct_count, first_value, last_value) = single_chunk_facts(chunks);

    let mut mass = v1::Bytemass {
        compressed_bytes: Some(compressed_bytes),
        uncompressed_bytes: Some(uncompressed_bytes),
        codec: uniform_codec(chunks),
        encodings,
        data_encodings,
        dictionary,
        dictionary_bytes,
        unencoded_bytes,
        value_count: Some(value_count),
        null_count,
        distinct_count,
        first_value,
        last_value,
        ..v1::Bytemass::default()
    };
    mass.bytes_per_value = ratio(mass.compressed_bytes, mass.value_count);
    mass.bytes_per_row = ratio(mass.compressed_bytes, Some(row_count));
    mass.compression_ratio = ratio(mass.uncompressed_bytes, mass.compressed_bytes);
    mass.null_fraction = ratio(mass.null_count, mass.value_count);
    mass.dictionary_fraction = ratio(mass.dictionary_bytes, mass.compressed_bytes);
    mass
}

/// The codec, when every chunk reports the same one.
fn uniform_codec(chunks: &[ColumnMass]) -> Option<String> {
    let first = chunks.first()?;
    chunks
        .iter()
        .all(|chunk| chunk.codec == first.codec)
        .then(|| first.codec.clone())
}

/// The union of encoding sets, in first-seen order.
fn union<'a>(sets: impl IntoIterator<Item = &'a [String]>) -> Vec<String> {
    let mut encodings = Vec::new();
    for set in sets {
        add_encodings(&mut encodings, set);
    }
    encodings
}

/// The union of encoding sets, when every chunk reports one.
fn known_union<'a>(sets: impl IntoIterator<Item = Option<&'a [String]>>) -> Option<Vec<String>> {
    let mut encodings = Vec::new();
    for set in sets {
        add_encodings(&mut encodings, set?);
    }
    (!encodings.is_empty()).then_some(encodings)
}

/// Add the encodings a set does not already hold.
///
/// The sets are tiny and their first-seen order is visible, so a `Vec` is
/// simpler than a `BTreeSet` here.
fn add_encodings(encodings: &mut Vec<String>, set: &[String]) {
    for encoding in set {
        if !encodings.contains(encoding) {
            encodings.push(encoding.clone());
        }
    }
}

/// The sum of parts, when every part is known and at least one exists.
fn known_sum(parts: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    let mut total: u64 = 0;
    let mut seen = false;
    for part in parts {
        total = total.checked_add(part?)?;
        seen = true;
    }
    seen.then_some(total)
}

/// Distinct, first, and last are file facts only with one chunk: summing
/// distinct counts double-counts values, and the first/last chunk is not the
/// file's bounds unless the data is ordered.
fn single_chunk_facts(chunks: &[ColumnMass]) -> (Option<u64>, Option<String>, Option<String>) {
    if chunks.len() != 1 {
        return (None, None, None);
    }
    let chunk = &chunks[0];
    (
        chunk.distinct_count,
        chunk.min_value.clone(),
        chunk.max_value.clone(),
    )
}

/// `numerator / denominator`, when the denominator is non-zero.
fn ratio(numerator: Option<u64>, denominator: Option<u64>) -> Option<f64> {
    let denominator = denominator?;
    (denominator > 0).then(|| numerator.unwrap_or(0) as f64 / denominator as f64)
}

async fn read_input(lake: &Lake, uri: &str) -> Result<FileMass, Error> {
    if uri.contains("://") {
        return remote::read_remote(lake, uri).await;
    }
    Ok(read_file_masses(Path::new(uri))?)
}
