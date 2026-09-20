//! Aggregate byte-mass metadata across multiple physical Parquet files.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::parquet_helpers::{ColumnMass, Error, FileMass};

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
    pub fn file_mass(&self) -> FileMass {
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
pub struct MassAccumulator {
    file_count: usize,
    num_rows: u64,
    columns: BTreeMap<String, ColumnMassSummary>,
}

impl MassAccumulator {
    /// Create an empty byte-mass accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one physical Parquet file's footer metadata.
    ///
    /// # Errors
    /// Returns [`Error`] if a row or byte total exceeds its integer type.
    pub fn add(&mut self, mass: FileMass) -> Result<(), Error> {
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
    pub fn finish(self) -> MassSummary {
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
