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

/// A decoded row sample: column names and stringified nullable cells.
#[derive(Debug, Clone, Default)]
pub struct Sample {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
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
#[derive(Debug, Clone, Default, Serialize)]
pub struct ColumnMass {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// On-disk (compressed) bytes for this column chunk.
    pub compressed_bytes: u64,
    /// Encoded bytes before compression (including page headers).
    pub uncompressed_bytes: u64,
    /// Compression codec recorded in the column chunk metadata.
    pub codec: String,
    /// Encodings listed on the column chunk.
    pub encodings: Vec<String>,
    /// Values in this chunk (including nulls).
    // aipnaming: allow(aip-141/count-suffix)
    pub num_values: u64,
    /// Dictionary page offset is present.
    pub dictionary: bool,
    /// Footer `null_count`, when statistics exist.
    pub null_count: Option<u64>,
    /// Footer `distinct_count`, when statistics exist.
    pub distinct_count: Option<u64>,
    /// Footer min, when statistics exist.
    // aipnaming: allow(aip-145/ranges)
    pub min_value: Option<String>,
    /// Footer max, when statistics exist.
    // aipnaming: allow(aip-145/ranges)
    pub max_value: Option<String>,
    /// Physical type of the leaf column.
    pub physical_type: String,
    /// Row-group index (0-based).
    pub row_group: u32,
    /// Rows in this row group.
    pub row_group_rows: u64,
    /// Data pages in the OffsetIndex, when `--indexes` loaded one.
    pub page_count: Option<u64>,
    /// Sum of OffsetIndex `compressed_page_size`, when loaded.
    pub page_compressed_bytes: Option<u64>,
}

/// A file's byte masses, read purely from metadata.
#[derive(Debug, Clone, Default, Serialize)]
pub struct FileMass {
    /// Number of rows in the file (shared denominator for per-row mass).
    pub row_count: u64,
    /// Number of row groups.
    pub row_group_count: usize,
    /// Declared sort columns, as `name` or `name DESC`.
    pub sorting_columns: Vec<String>,
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

/// Read byte masses from a local file. `indexes` loads ColumnIndex/OffsetIndex
/// (one extra read of the page-index region). Default callers pass `false`.
///
/// # Errors
/// Returns [`Error`] if `path` is not a readable parquet file.
pub fn read_file_masses(path: &Path, indexes: bool) -> Result<FileMass, Error> {
    super::r#impl::read_file_masses(path, indexes)
}

/// Lowest ColumnIndex/OffsetIndex offset recorded in `footer`, when present.
///
/// # Errors
/// Returns [`Error`] if `footer` is not a valid Parquet footer.
pub fn page_index_start(footer: &[u8]) -> Result<Option<u64>, Error> {
    super::r#impl::page_index_start(footer)
}

/// Read byte masses from a file suffix that includes the footer and, when
/// present, the page-index region. `tail` must end at `file_size`.
///
/// # Errors
/// Returns [`Error`] if `tail` is not a valid Parquet suffix.
pub fn read_tail_masses(tail: &[u8], file_size: u64) -> Result<FileMass, Error> {
    super::r#impl::read_tail_masses(tail, file_size)
}

/// Read up to `max_rows` leading rows of a local Parquet file, decoding every
/// value to a string (nulls stay `None`). `None` reads all rows.
///
/// # Errors
/// Returns [`Error`] if `path` is not a readable Parquet file or decoding a
/// value fails.
pub fn read_sample(path: &Path, max_rows: Option<usize>) -> Result<Sample, Error> {
    super::r#impl::read_sample(path, max_rows)
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
        assert!(mass.row_group_count > 0);
        assert!(mass
            .columns
            .iter()
            .all(|column| !column.physical_type.is_empty()
                && !column.encodings.is_empty()
                && column.num_values > 0
                && column.row_group_rows > 0));
    }

    #[test]
    fn read_sample_decodes_bounded_rows() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_reddit_none.parquet"
        );
        let sample = read_sample(Path::new(path), Some(16)).unwrap();
        assert!(!sample.columns.is_empty(), "expected columns");
        assert_eq!(sample.rows.len(), 16);
        for row in &sample.rows {
            assert_eq!(row.len(), sample.columns.len());
        }
        assert!(
            sample
                .rows
                .iter()
                .flatten()
                .any(std::option::Option::is_some),
            "expected at least one decoded value"
        );
    }

    #[test]
    fn read_file_masses_indexes_is_optional() {
        let path = Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_snappy.parquet"
        ));
        let without = read_file_masses(path, false).unwrap();
        let with = read_file_masses(path, true).unwrap();
        assert_eq!(without.columns.len(), with.columns.len());
        assert!(without
            .columns
            .iter()
            .all(|column| column.page_count.is_none()));
    }
}
