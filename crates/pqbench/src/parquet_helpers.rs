//! Public parquet page-parsing interface.
//!
//! Everything pqbench needs from a parquet file — the encoded page payloads a
//! codec would compress — is exposed through our own types here. No `parquet`
//! crate type ever appears in this module's API, so swapping the backing
//! parser (currently [`default_parser`]) won't touch tool code.
//!
//! Only NONE-compressed input is supported: `Page.payload` is the raw encoded
//! values (what `compression.rs` will sweep codecs over).

use std::path::Path;

/// One encoded page: the payload a codec compresses, plus its metadata.
#[derive(Debug, Clone)]
pub struct Page {
    /// The encoded values for this page (uncompressed; NONE input).
    pub payload: Vec<u8>,
    /// Number of values in this page.
    pub num_values: u32,
    /// True for a dictionary page (first page of a dictionary-encoded chunk).
    pub is_dictionary: bool,
}

/// A column chunk: the pages of one column in one row group.
#[derive(Debug)]
pub struct ColumnChunk {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    pub pages: Vec<Page>,
}

/// A parsed parquet file: one chunk per column per row group.
#[derive(Debug)]
pub struct ParquetFile {
    pub chunks: Vec<ColumnChunk>,
}

/// Errors from the parquet layer.
#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "parquet: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// Anything that can extract pages from an in-memory parquet byte buffer.
pub trait PageParser {
    /// Extract the encoded pages of a parquet file from an in-memory buffer.
    ///
    /// # Errors
    /// Returns [`Error`] if `bytes` is not a valid parquet file, or if any
    /// column is compressed (only NONE input is supported).
    fn parse_pages(&self, bytes: &[u8]) -> Result<ParquetFile, Error>;
}

/// The bundled page parser. Backed by parquet-rs; see the private
/// `parquet_impl` module for the implementation.
pub fn default_parser() -> impl PageParser {
    crate::parquet_impl::ParquetRsParser
}

/// A column's byte mass, read from parquet metadata (no page decoding).
#[derive(Debug, Clone)]
pub struct ColumnMass {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub path: String,
    /// On-disk (compressed) bytes for this column chunk.
    pub bytes: u64,
}

/// A file's byte masses, read purely from metadata.
#[derive(Debug)]
pub struct FileMass {
    /// Number of rows in the file (shared denominator for per-row mass).
    pub num_rows: u64,
    /// One entry per column chunk (per row group), in file order.
    pub columns: Vec<ColumnMass>,
}

/// Anything that can read byte masses from a parquet file's footer.
///
/// Unlike [`PageParser`], this reads only the file footer metadata, so it works
/// on any parquet file regardless of column compression.
pub trait MetadataParser {
    /// Read a file's byte masses from its footer metadata.
    ///
    /// # Errors
    /// Returns [`Error`] if `path` is not a readable parquet file.
    fn read_masses(&self, path: &Path) -> Result<FileMass, Error>;
}

/// The bundled metadata parser. Backed by parquet-rs.
pub fn default_metadata_parser() -> impl MetadataParser {
    crate::parquet_impl::ParquetRsParser
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_none_file_extracts_pages() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_reddit_none.parquet"
        );
        let buf = std::fs::read(path).expect("missing NONE fixture");

        let file = default_parser().parse_pages(&buf).unwrap();
        assert!(!file.chunks.is_empty());
        for chunk in &file.chunks {
            assert!(!chunk.pages.is_empty());
            for page in &chunk.pages {
                assert!(!page.payload.is_empty());
            }
        }
        // The dominant text column should produce multi-MB of pages.
        let total: usize = file
            .chunks
            .iter()
            .flat_map(|c| &c.pages)
            .map(|p| p.payload.len())
            .sum();
        assert!(total > 1_000_000, "total payload {total}");
    }

    #[test]
    fn compressed_input_is_rejected_with_clear_error() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_snappy.parquet"
        );
        let buf = std::fs::read(path).expect("missing snappy fixture");
        let err = default_parser().parse_pages(&buf).unwrap_err();
        assert!(
            err.to_string().contains("NONE"),
            "expected a clear NONE-only error, got: {err}"
        );
    }

    #[test]
    fn read_masses_works_on_compressed_file() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_snappy.parquet"
        );
        let mass = default_metadata_parser()
            .read_masses(Path::new(path))
            .unwrap();
        assert!(mass.num_rows > 0);
        assert!(!mass.columns.is_empty());
        assert!(
            mass.columns.iter().all(|c| c.bytes > 0),
            "expected positive on-disk column bytes"
        );
    }
}
