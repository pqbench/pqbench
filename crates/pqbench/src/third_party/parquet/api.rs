//! Public parquet page-parsing interface.
//!
//! Everything pqbench needs from a parquet file — the encoded page payloads a
//! codec would compress — is exposed through our own types here. No `parquet`
//! crate type ever appears in this module's API, so swapping the backing
//! parser (currently [`default_parser`]) won't touch tool code.
//!
//! Only NONE-compressed input is supported: `Page.payload` is the raw encoded
//! values (what `compression.rs` will sweep codecs over).

use std::ops::Range;
use std::path::{Path, PathBuf};

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

/// One Parquet object to read or copy, as plain data.
///
/// Storage is the caller's job: a local file is read in place, while a remote
/// object is fetched as its footer and row-group byte ranges and passed as
/// [`ObjectSource::Partial`]. No storage type appears here, so this module
/// stays one third-party crate.
#[derive(Debug, Clone)]
pub enum ObjectSource {
    /// A local filesystem path, opened and seeked by the reader.
    Path(PathBuf),
    /// Pre-fetched byte ranges of a remote object.
    Partial {
        /// Total object size in bytes.
        size: u64,
        /// `(offset, bytes)` ranges covering the footer and the kept rows.
        parts: Vec<(u64, Vec<u8>)>,
    },
}

/// Rows read from one Parquet file: column names and one value list per row.
#[derive(Debug, Clone)]
pub struct FileRows {
    /// Column names, in file order.
    pub columns: Vec<String>,
    /// Row values aligned to [`FileRows::columns`].
    pub rows: Vec<Vec<Value>>,
}

/// Read rows from one source, keeping at most the first `row_groups`.
///
/// `None` reads every row group.
///
/// # Errors
/// Fails when the source cannot be read or a row cannot be decoded.
pub fn read_rows(source: &ObjectSource, row_groups: Option<usize>) -> Result<FileRows, Error> {
    super::r#impl::read_rows(source, row_groups)
}

/// Copy the selected row groups of `sources` into one Parquet buffer.
///
/// Copied pages keep their source encodings; newly written columns default to
/// zstd.
///
/// # Errors
/// Fails when there are no sources, a source cannot be read, schemas differ, or
/// the writer cannot emit the file.
pub fn write_parquet(
    sources: &[ObjectSource],
    row_groups: Option<usize>,
) -> Result<Vec<u8>, Error> {
    super::r#impl::write_parquet(sources, row_groups)
}

/// The footer byte range of a Parquet object of `size`, from its trailer.
///
/// `trailer` is the object's last eight bytes: the four-byte metadata length
/// followed by `PAR1`. The range covers the serialized metadata and the
/// trailer, which is what a metadata parser expects.
///
/// # Errors
/// Fails when `trailer` is not a Parquet trailer or the length is invalid.
pub fn footer_range(size: u64, trailer: &[u8]) -> Result<Range<u64>, Error> {
    super::r#impl::footer_range(size, trailer)
}

/// The data byte ranges to fetch for the first `row_groups` groups.
///
/// `footer` is the [`footer_range`] bytes. `None` covers every row group.
///
/// # Errors
/// Fails when `footer` is not valid Parquet metadata.
pub fn data_ranges(footer: &[u8], row_groups: Option<usize>) -> Result<Vec<Range<u64>>, Error> {
    super::r#impl::data_ranges(footer, row_groups)
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
