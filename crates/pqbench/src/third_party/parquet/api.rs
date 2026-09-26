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

use serde::Serialize;
use serde_json::Value;

/// One encoded page: the payload a codec compresses, plus its metadata.
#[derive(Debug, Clone)]
pub struct Page {
    /// The encoded values for this page (uncompressed; NONE input).
    pub payload: Vec<u8>,
    /// Number of values in this page.
    pub value_count: u32,
    /// True for a dictionary page (first page of a dictionary-encoded chunk).
    pub dictionary: bool,
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
/// `impl` module for the implementation.
pub fn default_parser() -> impl PageParser {
    super::r#impl::ParquetRsParser
}

/// A column's byte mass, read from parquet metadata (no page decoding).
#[derive(Debug, Clone, Serialize)]
pub struct ColumnMass {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// On-disk (compressed) bytes for this column chunk.
    pub compressed_bytes: u64,
    /// Encoded bytes before compression (including page headers).
    pub uncompressed_bytes: u64,
    /// Compression codec recorded in the column chunk metadata.
    pub codec: String,
}

/// A file's byte masses, read purely from metadata.
#[derive(Debug, Clone, Serialize)]
pub struct FileMass {
    /// Number of rows in the file (shared denominator for per-row mass).
    pub row_count: u64,
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
    super::r#impl::ParquetRsParser
}

/// Read byte masses from a complete Parquet footer.
///
/// `footer` must contain the serialized Thrift metadata followed by the
/// eight-byte Parquet footer trailer. Storage adapters that fetch only the end
/// of a Parquet object use this.
///
/// # Errors
/// Returns [`Error`] if `footer` is not a valid Parquet footer.
pub fn read_footer_masses(footer: &[u8]) -> Result<FileMass, Error> {
    super::r#impl::read_footer_masses(footer)
}

/// One Parquet object to read or copy: a URI/path and its storage options.
#[derive(Debug, Clone, Copy)]
pub struct Source<'a> {
    /// URI or filesystem path to the Parquet object.
    pub uri: &'a str,
    /// Storage options (`AWS_*`); ignored for a local path.
    pub env: &'a [(String, String)],
}

/// Rows read from one Parquet file: column names and one value list per row.
#[derive(Debug, Clone)]
pub struct FileRows {
    /// Column names, in file order.
    pub columns: Vec<String>,
    /// Row values aligned to [`FileRows::columns`].
    pub rows: Vec<Vec<Value>>,
}

/// Read rows from one Parquet object, keeping at most the first `row_groups`.
///
/// `None` reads every row group. Local objects are seeked; `s3://` objects
/// fetch only the footer and the selected row groups and need the `aws`
/// feature.
///
/// # Errors
/// Fails when the object cannot be read or a row cannot be decoded.
pub async fn read_rows(source: &Source<'_>, row_groups: Option<usize>) -> Result<FileRows, Error> {
    super::r#impl::read_rows(source, row_groups).await
}

/// Copy the selected row groups of `sources` into one Parquet buffer.
///
/// Copied pages keep their source encodings; newly written columns default to
/// zstd.
///
/// # Errors
/// Fails when there are no sources, a source cannot be read, schemas differ, or
/// the writer cannot emit the file.
pub async fn write_parquet(
    sources: &[Source<'_>],
    row_groups: Option<usize>,
) -> Result<Vec<u8>, Error> {
    super::r#impl::write_parquet(sources, row_groups).await
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
        assert!(mass.row_count > 0);
        assert!(!mass.columns.is_empty());
        assert!(
            mass.columns.iter().all(|c| c.compressed_bytes > 0),
            "expected positive on-disk column bytes"
        );
    }
}
