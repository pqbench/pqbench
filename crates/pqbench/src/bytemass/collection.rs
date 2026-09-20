//! Aggregate byte-mass metadata across multiple physical Parquet files.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::parquet_helpers::{
    default_metadata_parser, ColumnMass, Error, FileMass, MetadataParser,
};

use super::command::FileMassRecord;
use super::remote;

/// One column's byte mass summed across a collection of Parquet files.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct ColumnMassSummary {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub path: String,
    /// Total on-disk bytes across all physical files.
    pub compressed_bytes: u64,
    /// Total encoded bytes before compression across all physical files.
    pub uncompressed_bytes: u64,
    /// Compression codecs present in the column chunks.
    pub codecs: BTreeSet<String>,
}

/// Byte masses summed across a collection of Parquet files.
#[derive(Debug, Clone, Default, Serialize)]
#[non_exhaustive]
pub struct MassSummary {
    /// Number of physical Parquet files included in the summary.
    pub file_count: usize,
    /// Total physical rows across all files.
    pub num_rows: u64,
    /// Per-column byte totals and codecs.
    pub columns: Vec<ColumnMassSummary>,
}

impl MassSummary {
    /// Convert the summary to the existing byte-mass analytics input.
    #[must_use]
    pub(super) fn file_mass(&self) -> FileMass {
        FileMass {
            num_rows: self.num_rows,
            columns: self
                .columns
                .iter()
                .map(|column| ColumnMass {
                    path: column.path.clone(),
                    bytes: column.compressed_bytes,
                    uncompressed_bytes: column.uncompressed_bytes,
                    codec: column.codecs.iter().cloned().collect::<Vec<_>>().join(","),
                })
                .collect(),
        }
    }
}

/// Incrementally aggregate Parquet metadata from any file source.
///
/// Local paths, Delta snapshots, and future object-store adapters can all feed
/// this type after obtaining one [`FileMass`] at a time.
#[derive(Debug, Default)]
struct MassAccumulator {
    file_count: usize,
    num_rows: u64,
    columns: BTreeMap<String, ColumnMassSummary>,
}

impl MassAccumulator {
    /// Create an empty byte-mass accumulator.
    #[must_use]
    fn new() -> Self {
        Self::default()
    }

    /// Add one physical Parquet file's footer metadata.
    ///
    /// # Errors
    /// Returns [`Error`] if a row or byte total exceeds its integer type.
    fn add(&mut self, mass: FileMass) -> Result<(), Error> {
        self.num_rows = checked_sum(self.num_rows, mass.num_rows)?;
        self.file_count = self
            .file_count
            .checked_add(1)
            .ok_or_else(|| Error("file count exceeds usize".into()))?;
        for column in mass.columns {
            let total =
                self.columns
                    .entry(column.path.clone())
                    .or_insert_with(|| ColumnMassSummary {
                        path: column.path,
                        compressed_bytes: 0,
                        uncompressed_bytes: 0,
                        codecs: BTreeSet::new(),
                    });
            total.compressed_bytes = checked_sum(total.compressed_bytes, column.bytes)?;
            total.uncompressed_bytes =
                checked_sum(total.uncompressed_bytes, column.uncompressed_bytes)?;
            total.codecs.insert(column.codec);
        }
        Ok(())
    }

    /// Finish aggregation and return the accumulated summary.
    #[must_use]
    fn finish(self) -> MassSummary {
        MassSummary {
            file_count: self.file_count,
            num_rows: self.num_rows,
            columns: self.columns.into_values().collect(),
        }
    }
}

fn checked_sum(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_add(right)
        .ok_or_else(|| Error("metadata totals exceed u64".into()))
}

/// A measured input collection: per-file records, the aggregate summary, and
/// the label the analytics tree is rooted at.
pub(super) struct MeasuredInputs {
    /// Per-input measured masses, in input order.
    pub files: Vec<FileMassRecord>,
    /// Per-column totals across all inputs.
    pub summary: MassSummary,
    /// File name for one input, or `N parquet files` for a collection.
    pub label: String,
}

/// Expand the inputs, measure each file's footer, and aggregate the collection.
pub(super) async fn measure_inputs(inputs: &[String]) -> Result<MeasuredInputs, Error> {
    let paths = expand_inputs(inputs)?;
    let label = collection_label(&paths);
    let mut accumulator = MassAccumulator::new();
    let mut files = Vec::with_capacity(paths.len());
    for path in &paths {
        let input = path.to_string_lossy().into_owned();
        let (size, mass) = read_input(&input).await?;
        accumulator.add(mass.clone())?;
        files.push(FileMassRecord {
            path: input,
            size,
            mass,
        });
    }
    Ok(MeasuredInputs {
        files,
        summary: accumulator.finish(),
        label,
    })
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

fn collection_label(paths: &[PathBuf]) -> String {
    if let [path] = paths {
        return path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".into());
    }
    format!("{} parquet files", paths.len())
}

async fn read_input(input: &str) -> Result<(u64, FileMass), Error> {
    if input.contains("://") {
        return remote::read_remote(input).await;
    }
    let path = Path::new(input);
    let size = std::fs::metadata(path)
        .map_err(|e| Error(format!("cannot stat {input}: {e}")))?
        .len();
    let mass = default_metadata_parser().read_masses(path)?;
    Ok((size, mass))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulator_combines_files_and_column_codecs() {
        let file = || FileMass {
            num_rows: 3,
            columns: vec![ColumnMass {
                path: "value".into(),
                bytes: 12,
                uncompressed_bytes: 24,
                codec: "SNAPPY".into(),
            }],
        };
        let mut accumulator = MassAccumulator::new();
        accumulator.add(file()).unwrap();
        accumulator.add(file()).unwrap();
        let summary = accumulator.finish();
        assert_eq!(summary.file_count, 2);
        assert_eq!(summary.num_rows, 6);
        assert_eq!(summary.columns[0].compressed_bytes, 24);
        assert_eq!(summary.columns[0].uncompressed_bytes, 48);
        assert_eq!(summary.columns[0].codecs, BTreeSet::from(["SNAPPY".into()]));
        assert_eq!(summary.file_mass().columns[0].codec, "SNAPPY");
    }

    #[test]
    fn file_mass_preserves_multiple_codecs() {
        let mut accumulator = MassAccumulator::new();
        for codec in ["ZSTD", "SNAPPY"] {
            accumulator
                .add(FileMass {
                    num_rows: 1,
                    columns: vec![ColumnMass {
                        path: "value".into(),
                        bytes: 1,
                        uncompressed_bytes: 2,
                        codec: codec.into(),
                    }],
                })
                .unwrap();
        }
        let mass = accumulator.finish().file_mass();
        assert_eq!(mass.columns[0].codec, "SNAPPY,ZSTD");
    }
}
