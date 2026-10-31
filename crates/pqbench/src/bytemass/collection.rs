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

/// The running fold of one column's chunks.
struct Fold {
    mass: v1::Bytemass,
    chunks: usize,
    codec: Option<String>,
    codec_mixed: bool,
}

impl Fold {
    fn new() -> Self {
        Self {
            mass: v1::Bytemass {
                null_count: Some(0),
                ..v1::Bytemass::default()
            },
            chunks: 0,
            codec: None,
            codec_mixed: false,
        }
    }

    /// Fold one chunk into the column.
    fn add(&mut self, chunk: ColumnMass) {
        self.chunks += 1;
        let mass = &mut self.mass;
        mass.compressed_bytes = add(mass.compressed_bytes, chunk.compressed_bytes);
        mass.uncompressed_bytes = add(mass.uncompressed_bytes, chunk.uncompressed_bytes);
        mass.value_count = add(mass.value_count, chunk.num_values);
        match &self.codec {
            None => self.codec = Some(chunk.codec),
            Some(codec) if *codec != chunk.codec => self.codec_mixed = true,
            Some(_) => {}
        }
        mass.dictionary |= chunk.dictionary;
        for encoding in chunk.encodings {
            if !mass.encodings.contains(&encoding) {
                mass.encodings.push(encoding);
            }
        }
        // A group with no statistics makes the file total unknown, not smaller.
        mass.null_count = match (mass.null_count, chunk.null_count) {
            (_, None) => None,
            (Some(total), Some(count)) => total.checked_add(count),
            (None, Some(_)) => None,
        };
        // First/last and distinct are file facts only when there is one chunk:
        // summing distinct counts double-counts values, and the first/last
        // chunk is not the file's min/max unless the data is ordered.
        if self.chunks == 1 {
            mass.distinct_count = chunk.distinct_count;
            mass.first_value = chunk.min_value;
            mass.last_value = chunk.max_value;
        } else {
            mass.distinct_count = None;
            mass.first_value = None;
            mass.last_value = None;
        }
    }

    /// The folded mass, with the ratios and the single codec resolved.
    fn finish(mut self) -> v1::Bytemass {
        self.mass.codec = (!self.codec_mixed).then_some(self.codec).flatten();
        self.mass.bytes_per_value = ratio(self.mass.compressed_bytes, self.mass.value_count);
        self.mass.compression_ratio =
            ratio(self.mass.uncompressed_bytes, self.mass.compressed_bytes);
        self.mass.null_fraction = ratio(self.mass.null_count, self.mass.value_count);
        self.mass
    }
}

/// Fold one `ColumnMass` per chunk into one [`v1::Column`] per leaf column.
fn columns(blob_name: &str, mass: FileMass) -> Result<Vec<v1::Column>, Error> {
    let (service, locator) = v1::name::parse_blob(blob_name)
        .ok_or_else(|| Error::Name(format!("{blob_name} is not a blobs/… name")))?;
    let mut at: BTreeMap<String, usize> = BTreeMap::new();
    let mut columns: Vec<v1::Column> = Vec::new();
    let mut folds: Vec<Fold> = Vec::new();
    for chunk in mass.columns {
        let index = *at.entry(chunk.column.clone()).or_insert_with(|| {
            columns.push(v1::Column {
                name: v1::name::column(&service, &locator, &chunk.column),
                row_group_count: Some(mass.row_group_count as u64),
                ..v1::Column::default()
            });
            folds.push(Fold::new());
            columns.len() - 1
        });
        folds[index].add(chunk);
    }
    for (column, fold) in columns.iter_mut().zip(folds) {
        column.mass = Some(fold.finish());
    }
    Ok(columns)
}

fn add(total: Option<u64>, part: u64) -> Option<u64> {
    total.unwrap_or(0).checked_add(part)
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
