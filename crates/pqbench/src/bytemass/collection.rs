//! Measure Parquet files into per-column byte masses.

use std::collections::BTreeMap;
use std::path::Path;

use crate::third_party::object_store::api::ObjectStat;
use crate::third_party::parquet::api::{read_file_masses, ColumnMass, FileMass};
use crate::v1;
use crate::Lake;

use super::api::MeasuredFile;
use super::remote;
use super::Error;

/// Expand the inputs, measure each file's footer, and fold its column chunks
/// into one [`v1::Bytemass`] per column.
pub(super) async fn measure_files(
    lake: &Lake,
    inputs: &[String],
    env: &BTreeMap<String, String>,
) -> Result<Vec<MeasuredFile>, Error> {
    let mut files = Vec::new();
    for uri in inputs {
        files.push(measure_file(lake, uri, env).await?);
    }
    Ok(files)
}

/// Measure one file: its footer read once, its chunks folded per column.
pub(super) async fn measure_file(
    lake: &Lake,
    uri: &str,
    env: &BTreeMap<String, String>,
) -> Result<MeasuredFile, Error> {
    let (stat, mass) = read_input(lake, uri, env).await?;
    Ok(measured_file(uri, &stat, mass))
}

/// Name a file's masses: its `Blob`, and each column's `Bytemass` under it.
fn measured_file(uri: &str, stat: &ObjectStat, mass: FileMass) -> MeasuredFile {
    let blob_name = v1::name::blob_name(uri);
    let columns = fold(&blob_name, mass.columns);
    let blob = v1::Blob {
        name: blob_name,
        size_bytes: Some(stat.size_bytes),
        storage_class: stat.storage_class.clone(),
        row_group_count: Some(mass.row_group_count as u64),
        column_count: Some(columns.len() as u64),
        ..v1::Blob::default()
    };
    MeasuredFile { blob, columns }
}

/// Fold one `ColumnMass` per chunk into one `Bytemass` per column.
fn fold(blob_name: &str, chunks: Vec<ColumnMass>) -> Vec<v1::Bytemass> {
    let mut at: BTreeMap<String, usize> = BTreeMap::new();
    let mut columns: Vec<v1::Bytemass> = Vec::new();
    for chunk in chunks {
        let index = *at.entry(chunk.column.clone()).or_insert_with(|| {
            columns.push(v1::Bytemass {
                name: v1::name::bytemass(blob_name, &chunk.column),
                ..v1::Bytemass::default()
            });
            columns.len() - 1
        });
        let column = &mut columns[index];
        column.compressed_bytes = add(column.compressed_bytes, chunk.compressed_bytes);
        column.uncompressed_bytes = add(column.uncompressed_bytes, chunk.uncompressed_bytes);
        column.value_count = add(column.value_count, chunk.num_values);
        column.codec.get_or_insert(chunk.codec);
        column.dictionary |= chunk.dictionary;
        for encoding in chunk.encodings {
            if !column.encodings.contains(&encoding) {
                column.encodings.push(encoding);
            }
        }
        if let Some(count) = chunk.null_count {
            column.null_count = add(column.null_count, count);
        }
        if let Some(count) = chunk.distinct_count {
            column.distinct_count = add(column.distinct_count, count);
        }
        if column.first_value.is_none() {
            column.first_value = chunk.min_value;
        }
        if chunk.max_value.is_some() {
            column.last_value = chunk.max_value;
        }
    }
    for column in &mut columns {
        column.bytes_per_value = ratio(column.compressed_bytes, column.value_count);
        column.compression_ratio = ratio(column.uncompressed_bytes, column.compressed_bytes);
        column.null_fraction = ratio(column.null_count, column.value_count);
    }
    columns
}

fn add(total: Option<u64>, part: u64) -> Option<u64> {
    Some(total.unwrap_or(0) + part)
}

/// `numerator / denominator`, when the denominator is non-zero.
fn ratio(numerator: Option<u64>, denominator: Option<u64>) -> Option<f64> {
    let denominator = denominator?;
    (denominator > 0).then(|| numerator.unwrap_or(0) as f64 / denominator as f64)
}

async fn read_input(
    lake: &Lake,
    input: &str,
    env: &BTreeMap<String, String>,
) -> Result<(ObjectStat, FileMass), Error> {
    if input.contains("://") {
        return remote::read_remote(
            lake,
            input,
            env.iter().map(|(key, value)| (key.clone(), value.clone())),
        )
        .await;
    }
    let mass = read_file_masses(Path::new(input))?;
    let stat = ObjectStat {
        size_bytes: mass.size_bytes,
        identity: None,
        storage_class: None,
    };
    Ok((stat, mass))
}
