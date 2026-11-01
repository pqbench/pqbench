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

/// The physical kind of a typed column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Boolean,
    Integer,
    Number,
    Bytes,
}

/// One typed cell: a null, bool, integer, float, or byte string.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Number(f64),
    Bytes(Vec<u8>),
}

/// A named column and its kind.
#[derive(Debug, Clone)]
pub struct TypedColumn {
    pub name: String,
    pub kind: Kind,
}

/// A decoded typed row sample: columns (with kinds) and nullable cells.
#[derive(Debug, Clone, Default)]
pub struct TypedSample {
    pub columns: Vec<TypedColumn>,
    pub rows: Vec<Vec<Value>>,
}

/// How to write a typed sample back to Parquet bytes.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// `uncompressed`, `snappy`, `gzip`, `lz4`, or `zstd`.
    pub compression: String,
    /// Codec level, when the codec takes one.
    pub level: Option<i32>,
    /// Enable dictionary encoding.
    pub dictionary: bool,
    /// Dictionary page size limit, when set.
    pub dictionary_bytes: Option<usize>,
    /// Default encoding for every column: `plain`, `delta`, `rle`,
    /// `delta_length`, `delta_byte_array`, or `byte_stream_split`.
    pub encoding: Option<String>,
    /// Data page size limit, when set.
    pub page_size: Option<usize>,
    /// Column names to record as the file's sorting columns.
    pub sorting_columns: Vec<String>,
    /// Rows per row group; 0 writes every row into one group.
    pub row_group_size: usize,
}

/// Errors from the parquet layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A file or stream could not be read.
    Io(std::io::Error),
    /// The parquet library failed.
    Parquet(Box<dyn std::error::Error + Send + Sync>),
    /// A caller argument was invalid.
    Invalid(String),
    /// A required page header was missing.
    Missing(&'static str),
    /// A value did not fit its type.
    Overflow(&'static str),
    /// An encoding was not usable for a column kind.
    Encoding(String),
    /// The input used a form this parser does not accept.
    Unsupported(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(_) => write!(f, "parquet i/o failed"),
            Error::Parquet(_) => write!(f, "parquet failed"),
            Error::Invalid(message) | Error::Encoding(message) | Error::Unsupported(message) => {
                write!(f, "{message}")
            }
            Error::Missing(what) => write!(f, "missing {what}"),
            Error::Overflow(what) => write!(f, "{what} overflowed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            Error::Parquet(error) => Some(error.as_ref()),
            Error::Invalid(_)
            | Error::Missing(_)
            | Error::Overflow(_)
            | Error::Encoding(_)
            | Error::Unsupported(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

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

/// One column chunk's byte mass, read from parquet metadata (no page decoding).
#[derive(Debug, Clone, Default)]
pub struct ColumnMass {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// The leaf column's physical type.
    pub physical_data_type: String,
    /// The leaf column's maximum definition level.
    pub max_definition_level: u16,
    /// The leaf column's maximum repetition level.
    pub max_repetition_level: u16,
    /// On-disk (compressed) bytes for this column chunk.
    pub compressed_bytes: u64,
    /// Encoded bytes before compression (including page headers).
    pub uncompressed_bytes: u64,
    /// Compression codec recorded in the column chunk metadata.
    pub codec: String,
    /// Encodings listed on the column chunk.
    pub encodings: Vec<String>,
    /// Encodings used by the chunk's data pages, when the writer recorded them.
    pub data_encodings: Option<Vec<String>>,
    /// Values in this chunk (including nulls).
    // aipnaming: allow(aip-141/count-suffix)
    pub num_values: u64,
    /// Dictionary page offset is present.
    pub dictionary: bool,
    /// Bytes from the dictionary page offset to the first data page, when a
    /// dictionary page is present.
    pub dictionary_bytes: Option<u64>,
    /// Variable-length value bytes before encoding, when the writer recorded them.
    pub unencoded_bytes: Option<u64>,
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
}

/// A file's byte masses, read purely from metadata: one entry per column chunk,
/// in file order. A consumer that wants one entry per column folds them.
#[derive(Debug, Clone, Default)]
pub struct FileMass {
    /// The file's size in bytes.
    pub size_bytes: u64,
    /// Number of rows in the file.
    pub row_count: u64,
    /// Number of row groups.
    pub row_group_count: usize,
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
/// eight-byte Parquet footer trailer, and `size_bytes` the object's size.
/// Storage adapters that fetch only the end of a Parquet object use this.
///
/// # Errors
/// Returns [`Error`] if `footer` is not a valid Parquet footer.
pub fn read_footer_masses(footer: &[u8], size_bytes: u64) -> Result<FileMass, Error> {
    super::r#impl::read_footer_masses(footer, size_bytes)
}

/// Read byte masses from a local file.
///
/// # Errors
/// Returns [`Error`] if `path` is not a readable parquet file.
pub fn read_file_masses(path: &Path) -> Result<FileMass, Error> {
    super::r#impl::read_file_masses(path)
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

/// Read up to `max_rows` leading rows of a local Parquet file as typed cells.
/// `None` reads all rows. A column's kind comes from its first non-null value.
///
/// # Errors
/// Returns [`Error`] if `path` is not a readable Parquet file or decoding a
/// value fails.
pub fn read_typed_sample(path: &Path, max_rows: Option<usize>) -> Result<TypedSample, Error> {
    super::r#impl::read_typed_sample(path, max_rows)
}

/// Write typed columns and rows to an in-memory Parquet file.
///
/// # Errors
/// Returns [`Error`] if the schema, codec, or writer fails.
pub fn write_parquet(
    columns: &[TypedColumn],
    rows: &[Vec<Value>],
    options: &WriteOptions,
) -> Result<Vec<u8>, Error> {
    super::r#impl::write_parquet(columns, rows, options)
}

/// Read byte masses from an in-memory Parquet file.
///
/// # Errors
/// Returns [`Error`] if `bytes` is not a valid Parquet file.
pub fn read_buffer_masses(bytes: &[u8]) -> Result<FileMass, Error> {
    super::r#impl::read_buffer_masses(bytes)
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
            .all(|column| !column.encodings.is_empty() && column.num_values > 0));
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
    fn read_sample_decodes_a_compressed_file() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_snappy.parquet"
        );
        let sample = read_sample(Path::new(path), None).unwrap();
        assert_eq!(sample.columns.len(), 1);
        assert_eq!(sample.rows.len(), 500, "snappy rows should all decode");
    }

    #[test]
    fn read_file_masses_returns_one_entry_per_column_chunk() {
        let path = Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_snappy.parquet"
        ));
        let mass = read_file_masses(path).unwrap();
        assert_eq!(mass.columns.len(), mass.row_group_count);
        assert!(mass.row_count > 0);
        assert!(mass
            .columns
            .iter()
            .all(|column| column.compressed_bytes > 0));
    }

    #[test]
    fn read_typed_sample_populates_columns_and_kinds() {
        let path = Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_reddit_none.parquet"
        ));
        let sample = read_typed_sample(path, Some(16)).unwrap();
        assert_eq!(sample.columns.len(), 7);
        assert_eq!(sample.rows.len(), 16);
        for row in &sample.rows {
            assert_eq!(row.len(), 7);
        }
        let text = sample
            .columns
            .iter()
            .find(|column| column.name == "text")
            .expect("text column");
        assert_eq!(text.kind, Kind::Bytes);
        assert!(matches!(sample.rows[0][0], Value::Bytes(_)));
    }

    #[test]
    fn write_and_read_buffer_masses_round_trip() {
        let columns = vec![
            TypedColumn {
                name: "id".into(),
                kind: Kind::Integer,
            },
            TypedColumn {
                name: "score".into(),
                kind: Kind::Number,
            },
            TypedColumn {
                name: "tag".into(),
                kind: Kind::Bytes,
            },
            TypedColumn {
                name: "flag".into(),
                kind: Kind::Boolean,
            },
        ];
        let rows: Vec<Vec<Value>> = (0..16)
            .map(|n| {
                vec![
                    Value::Integer(n),
                    Value::Number(n as f64 / 2.0),
                    Value::Bytes(format!("row-{n}").into_bytes()),
                    Value::Boolean(n % 2 == 0),
                ]
            })
            .collect();
        let options = WriteOptions {
            compression: "zstd".into(),
            level: None,
            dictionary: true,
            dictionary_bytes: None,
            encoding: None,
            page_size: None,
            sorting_columns: Vec::new(),
            row_group_size: 8,
        };
        let bytes = write_parquet(&columns, &rows, &options).unwrap();
        let mass = read_buffer_masses(&bytes).unwrap();
        assert_eq!(mass.row_count, 16);
        assert_eq!(mass.row_group_count, 2);
        assert_eq!(mass.columns.len(), 8);
        assert!(mass
            .columns
            .iter()
            .all(|column| column.compressed_bytes > 0));
    }

    #[test]
    fn read_typed_sample_decodes_a_compressed_file() {
        let path = Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/small_snappy.parquet"
        ));
        let sample = read_typed_sample(path, None).unwrap();
        assert_eq!(sample.columns.len(), 1);
        assert_eq!(sample.rows.len(), 500);
    }
}
