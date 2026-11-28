//! Public parquet page-parsing interface.
//!
//! Everything pqbench needs from a parquet file — the encoded page payloads a
//! codec would compress — is exposed through our own types here. No `parquet`
//! crate type ever appears in this module's API, so swapping the backing
//! parser won't touch tool code.
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
    /// The longest `min_value`/`max_value` a column chunk records; `None`
    /// keeps the writer's own default (64 bytes).
    pub stats_truncate_length: Option<usize>,
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
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            Error::Parquet(error) => Some(error.as_ref()),
            Error::Invalid(_) | Error::Encoding(_) | Error::Unsupported(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

/// Extract the encoded pages of a parquet file from an in-memory buffer.
///
/// # Errors
/// Returns [`Error`] if `bytes` is not a valid parquet file, or if any column
/// is compressed (only NONE input is supported).
pub fn parse_pages(bytes: &[u8]) -> Result<ParquetFile, Error> {
    super::r#impl::parse_pages(bytes)
}

/// One column chunk's byte mass, read from parquet metadata (no page decoding).
#[derive(Debug, Clone, Default)]
pub struct ColumnMass {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// The leaf column's physical type.
    pub physical_data_type: String,
    /// The leaf column's logical type, as the Parquet spec names it (`STRING`,
    /// `DECIMAL(10,2)`, `TIMESTAMP(MICROS,UTC)`, …), when the footer records
    /// one.
    pub logical_type: Option<String>,
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
    /// The chunk is dictionary-encoded: a dictionary page offset, dictionary
    /// data pages, or a dictionary encoding in the chunk's encoding set.
    pub dictionary: bool,
    /// Bytes from the dictionary page offset to the first data page, when the
    /// footer records an offset that locates the page.
    pub dictionary_bytes: Option<u64>,
    /// Data pages the footer counts as `PLAIN`-encoded, when it counts pages.
    pub plain_data_page_count: Option<u64>,
    /// Data pages the footer counts as dictionary-encoded, when it counts pages.
    pub dictionary_data_page_count: Option<u64>,
    /// Every chunk carries a page index (a column index and an offset index).
    pub page_index: bool,
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
    /// The serialized Thrift footer and its eight-byte trailer, in bytes: the
    /// metadata every column of the file pays for. Zero when the caller did not
    /// supply the footer's own length.
    pub footer_bytes: u64,
    /// The file's bytes outside every column chunk: the footer, the page
    /// indexes, bloom filters, key-value metadata (an `ARROW:schema`, say), and
    /// any other object bytes no chunk accounts for.
    pub unaccounted_bytes: u64,
    /// The writer the footer names (`created_by`), when it names one.
    pub writer: Option<String>,
    /// Number of rows in the file.
    pub row_count: u64,
    /// Number of row groups.
    pub row_group_count: usize,
    /// One entry per column chunk (per row group), in file order.
    pub columns: Vec<ColumnMass>,
}

/// Read byte masses from a complete Parquet footer.
///
/// `footer` must contain the serialized Thrift metadata followed by the
/// eight-byte Parquet footer trailer, and `size_bytes` the object's size, so
/// the record prices the metadata (the footer's own bytes) and every byte no
/// column chunk accounts for. Storage adapters that fetch only the end of a
/// Parquet object use this.
///
/// # Errors
/// Returns [`Error`] if `footer` is not a valid Parquet footer.
pub fn read_footer_masses(footer: &[u8], size_bytes: u64) -> Result<FileMass, Error> {
    super::r#impl::read_footer_masses(footer, size_bytes)
}

/// Read byte masses from a local file.
///
/// Only the footer is read: no page is decoded, so compressed columns are fine,
/// and large files aren't loaded into memory.
///
/// # Errors
/// Returns [`Error`] if `path` is not a readable parquet file.
pub fn read_file_masses(path: &Path) -> Result<FileMass, Error> {
    super::r#impl::read_file_masses(path)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A committed Parquet fixture, read by the tests that assert on real page
    /// and footer bytes rather than on bytes this crate wrote itself.
    fn fixture(name: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(name)
    }

    #[test]
    fn parse_none_file_extracts_pages() {
        let path = fixture("small_reddit_none.parquet");
        let buf = std::fs::read(path).expect("missing NONE fixture");

        let file = parse_pages(&buf).unwrap();
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
        let path = fixture("small_snappy.parquet");
        let buf = std::fs::read(path).expect("missing snappy fixture");
        let err = parse_pages(&buf).unwrap_err();
        assert!(
            err.to_string().contains("NONE"),
            "expected a clear NONE-only error, got: {err}"
        );
    }

    #[test]
    fn read_file_masses_works_on_compressed_file() {
        let path = fixture("small_snappy.parquet");
        let mass = read_file_masses(&path).unwrap();
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
    fn read_file_masses_returns_one_entry_per_column_chunk() {
        let path = fixture("small_snappy.parquet");
        let mass = read_file_masses(&path).unwrap();
        assert_eq!(mass.columns.len(), mass.row_group_count);
        assert!(mass.row_count > 0);
        assert!(mass
            .columns
            .iter()
            .all(|column| column.compressed_bytes > 0));
    }

    /// A written file's footer and the bytes no chunk accounts for are measured
    /// from the same read: the footer's own length and the object's size.
    #[test]
    fn a_written_file_accounts_for_its_footer() {
        let columns = vec![
            TypedColumn {
                name: "id".into(),
                kind: Kind::Integer,
            },
            TypedColumn {
                name: "tag".into(),
                kind: Kind::Bytes,
            },
        ];
        let rows: Vec<Vec<Value>> = (0..64)
            .map(|n| {
                vec![
                    Value::Integer(n),
                    Value::Bytes(format!("row-{n}").into_bytes()),
                ]
            })
            .collect();
        let options = WriteOptions {
            compression: "zstd".into(),
            level: None,
            dictionary: false,
            dictionary_bytes: None,
            encoding: None,
            page_size: None,
            stats_truncate_length: None,
            sorting_columns: Vec::new(),
            row_group_size: 32,
        };
        let bytes = write_parquet(&columns, &rows, &options).unwrap();
        let path = std::env::temp_dir().join(format!(
            "pqbench-accounting-{}-{:?}.parquet",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, &bytes).unwrap();
        let mass = read_file_masses(&path).unwrap();
        let size_bytes = std::fs::metadata(&path).unwrap().len();
        let _ = std::fs::remove_file(&path);

        assert_eq!(mass.size_bytes, size_bytes);
        assert_eq!(bytes.len() as u64, size_bytes);
        assert!(mass.footer_bytes > 0, "{mass:?}");
        assert!(mass.footer_bytes < size_bytes);
        let accounted: u64 = mass
            .columns
            .iter()
            .map(|column| column.compressed_bytes)
            .sum();
        assert_eq!(mass.unaccounted_bytes, size_bytes - accounted);
        // The footer is the largest part of what no chunk holds: the trailer
        // alone means at least eight bytes are always unaccounted for.
        assert!(mass.unaccounted_bytes >= mass.footer_bytes);

        // The same numbers from the footer slice alone, with no second read.
        let footer = &bytes[bytes.len() - mass.footer_bytes as usize..];
        let from_footer = read_footer_masses(footer, size_bytes).unwrap();
        assert_eq!(from_footer.footer_bytes, mass.footer_bytes);
        assert_eq!(from_footer.unaccounted_bytes, mass.unaccounted_bytes);
        assert_eq!(
            from_footer
                .columns
                .iter()
                .map(|column| column.compressed_bytes)
                .sum::<u64>(),
            accounted
        );
    }

    #[test]
    fn written_bytes_carry_the_expected_row_groups() {
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
            stats_truncate_length: None,
            sorting_columns: Vec::new(),
            row_group_size: 8,
        };
        let bytes = write_parquet(&columns, &rows, &options).unwrap();
        let path = std::env::temp_dir().join(format!(
            "pqbench-written-{}-{:?}.parquet",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, &bytes).unwrap();
        let mass = read_file_masses(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(mass.row_count, 16);
        assert_eq!(mass.row_group_count, 2);
        assert_eq!(mass.columns.len(), 8);
        assert!(mass
            .columns
            .iter()
            .all(|column| column.compressed_bytes > 0));
    }
}
