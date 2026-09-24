//! Raw layer: carry a file's per-column byte masses, read from metadata.
//!
//! `read` is a thin seam over [`crate::third_party::parquet::api::FileMass`]: the file's
//! per-chunk (path, on-disk bytes) entries, un-aggregated. `analytics` sums
//! chunks across row groups and derives the per-row measure.

use crate::third_party::parquet::api::FileMass;

/// One column chunk's raw on-disk byte mass.
pub(super) struct RawColumn {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// On-disk (compressed) bytes for this column chunk.
    pub compressed_bytes: u64,
}

/// A file's raw column masses, plus the row count that normalizes them.
pub(super) struct FileRaw {
    /// Number of rows in the file (shared denominator for per-row mass).
    pub row_count: u64,
    /// One entry per column chunk, in file order.
    pub columns: Vec<RawColumn>,
}

/// Lift parsed metadata into the raw per-chunk masses.
pub(super) fn read(mass: &FileMass) -> FileRaw {
    FileRaw {
        row_count: mass.row_count,
        columns: mass
            .columns
            .iter()
            .map(|c| RawColumn {
                column: c.column.clone(),
                compressed_bytes: c.compressed_bytes,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::third_party::parquet::api::ColumnMass;

    #[test]
    fn read_carries_per_chunk_bytes_and_rows() {
        let mass = FileMass {
            row_count: 100,
            columns: vec![
                ColumnMass {
                    column: "text".into(),
                    compressed_bytes: 40,
                    uncompressed_bytes: 80,
                    codec: "SNAPPY".into(),
                },
                ColumnMass {
                    column: "a.b".into(),
                    compressed_bytes: 60,
                    uncompressed_bytes: 120,
                    codec: "SNAPPY".into(),
                },
            ],
        };
        let raw = read(&mass);
        assert_eq!(raw.row_count, 100);
        assert_eq!(raw.columns.len(), 2);
        assert_eq!(raw.columns[0].column, "text");
        assert_eq!(raw.columns[0].compressed_bytes, 40);
        assert_eq!(raw.columns[1].column, "a.b");
        assert_eq!(raw.columns[1].compressed_bytes, 60);
    }
}
