//! Aggregate byte-mass metadata across multiple physical Parquet files.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::third_party::object_store::api::ObjectStat;
use crate::third_party::parquet::api::{
    default_metadata_parser, ColumnMass, Error, FileMass, MetadataParser,
};

use super::api::MassRow;
use super::remote;

/// One column's byte mass summed across a collection of Parquet files.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct ColumnMassSummary {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// Total on-disk bytes across all physical files.
    pub compressed_bytes: u64,
    /// Total encoded bytes before compression across all physical files.
    pub uncompressed_bytes: u64,
    /// Compression codecs present in the column chunks.
    pub codecs: BTreeSet<String>,
}

/// Byte masses summed across a collection of Parquet files.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct MassSummary {
    /// Number of physical Parquet files included in the summary.
    pub file_count: usize,
    /// Total physical rows across all files.
    pub row_count: u64,
    /// Per-column byte totals and codecs.
    pub columns: Vec<ColumnMassSummary>,
}

impl MassSummary {
    /// Convert the summary to the existing byte-mass analytics input.
    #[must_use]
    pub(super) fn file_mass(&self) -> FileMass {
        FileMass {
            row_count: self.row_count,
            columns: self
                .columns
                .iter()
                .map(|column| ColumnMass {
                    column: column.column.clone(),
                    compressed_bytes: column.compressed_bytes,
                    uncompressed_bytes: column.uncompressed_bytes,
                    codec: column.codecs.iter().cloned().collect::<Vec<_>>().join(","),
                })
                .collect(),
        }
    }
}

/// Expand the inputs, measure each file's footer, and flatten the collection
/// into one row per column chunk.
pub(super) async fn measure_inputs(
    inputs: &[String],
    env: &BTreeMap<String, String>,
) -> Result<Vec<MassRow>, Error> {
    let paths = expand_inputs(inputs)?;
    let mut rows = Vec::new();
    for path in &paths {
        let input = path.to_string_lossy().into_owned();
        let (stat, mass) = read_input(&input, env).await?;
        let row_count = mass.row_count;
        for column in mass.columns {
            rows.push(MassRow {
                uri: input.clone(),
                size_bytes: stat.size_bytes,
                row_count,
                column: column.column,
                compressed_bytes: column.compressed_bytes,
                uncompressed_bytes: column.uncompressed_bytes,
                codec: column.codec,
                storage_class: stat.storage_class.clone(),
            });
        }
    }
    Ok(rows)
}

fn expand_inputs(inputs: &[String]) -> Result<Vec<PathBuf>, Error> {
    let mut paths = BTreeSet::new();
    for input in inputs {
        if has_glob_metachar(input) && !input.contains("://") {
            let mut matched = false;
            for entry in glob::glob(&escape_literal_brackets(input))
                .map_err(|e| Error(format!("invalid mask {input}: {e}")))?
            {
                let path = entry.map_err(|e| Error(format!("cannot expand mask {input}: {e}")))?;
                paths.insert(path);
                matched = true;
            }
            if !matched {
                return Err(Error(format!("mask matched no files: {input}")));
            }
        } else {
            paths.insert(PathBuf::from(input));
        }
    }
    Ok(paths.into_iter().collect())
}

fn has_glob_metachar(input: &str) -> bool {
    input.contains(['*', '?'])
}

fn escape_literal_brackets(input: &str) -> String {
    input.replace('[', "[[]")
}

async fn read_input(
    input: &str,
    env: &BTreeMap<String, String>,
) -> Result<(ObjectStat, FileMass), Error> {
    if input.contains("://") {
        return remote::read_remote(
            input,
            env.iter().map(|(key, value)| (key.clone(), value.clone())),
        )
        .await;
    }
    let path = Path::new(input);
    let metadata =
        std::fs::metadata(path).map_err(|e| Error(format!("cannot stat {input}: {e}")))?;
    let stat = ObjectStat {
        size_bytes: metadata.len(),
        identity: None,
        storage_class: None,
    };
    let mass = default_metadata_parser().read_masses(path)?;
    Ok((stat, mass))
}
