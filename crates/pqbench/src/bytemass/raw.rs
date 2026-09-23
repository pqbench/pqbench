//! Raw layer: carry a file's per-column byte masses, read from metadata.
//!
//! `read` is a thin seam over [`crate::parquet_helpers::FileMass`]: the file's
//! per-chunk (path, on-disk bytes) entries, un-aggregated. `analytics` sums
//! chunks across row groups and derives the per-row measure.

use crate::parquet_helpers::FileMass;

/// One column chunk's raw on-disk byte mass.
pub(super) struct RawColumn {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub path: String,
    /// On-disk (compressed) bytes for this column chunk.
    pub bytes: u64,
}

/// A file's raw column masses, plus the row count that normalizes them.
pub(super) struct FileRaw {
    /// Number of rows in the file (shared denominator for per-row mass).
    pub num_rows: u64,
    /// One entry per column chunk, in file order.
    pub columns: Vec<RawColumn>,
}

/// Lift parsed metadata into the raw per-chunk masses.
pub(super) fn read(mass: &FileMass) -> FileRaw {
    FileRaw {
        num_rows: mass.num_rows,
        columns: mass
            .columns
            .iter()
            .map(|c| RawColumn {
                path: c.path.clone(),
                bytes: c.bytes,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_helpers::ColumnMass;

    #[test]
    fn read_carries_per_chunk_bytes_and_rows() {
        let mass = FileMass {
            num_rows: 100,
            columns: vec![
                ColumnMass {
                    path: "text".into(),
                    bytes: 40,
                    uncompressed_bytes: 80,
                    codec: "SNAPPY".into(),
                    ..ColumnMass::default()
                },
                ColumnMass {
                    path: "a.b".into(),
                    bytes: 60,
                    uncompressed_bytes: 120,
                    codec: "SNAPPY".into(),
                    ..ColumnMass::default()
                },
            ],
            ..FileMass::default()
        };
        let raw = read(&mass);
        assert_eq!(raw.num_rows, 100);
        assert_eq!(raw.columns.len(), 2);
        assert_eq!(raw.columns[0].path, "text");
        assert_eq!(raw.columns[0].bytes, 40);
        assert_eq!(raw.columns[1].path, "a.b");
        assert_eq!(raw.columns[1].bytes, 60);
    }
}
