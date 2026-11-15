//! `lint`: the storage a column's values do not need.
//!
//! The first rule prices UUID text stored as 36-byte strings. The footer gives
//! the column's on-disk bytes; a decoded sample gives its value shape and lets
//! the rule encode the same values as 16-byte binary. A suggestion is emitted
//! only when that binary sample is smaller, so repeated UUIDs are priced below
//! the 16-byte floor they would have if every value were unique.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use serde::Serialize;

use crate::third_party::parquet::api::{
    read_buffer_masses, read_file_masses, read_leaf_sample, write_parquet, ColumnMass,
    Error as ParquetError, Kind, TypedColumn, Value, WriteOptions,
};
use crate::v1;

/// Leading rows decoded to judge a column's values and price the rewrite.
pub const SAMPLE_ROWS: usize = 8192;

/// A column is UUID text when at least this fraction of its sampled values
/// match the canonical 36-byte shape.
const UUID_FRACTION: f64 = 0.99;

/// Suggestions smaller than this are noise: a handful of values is not worth a
/// schema change.
const MIN_SAVINGS_BYTES: u64 = 64 * 1024;

/// The rule id a suggestion carries.
pub const UUID_TEXT_RULE: &str = "uuid-text";

/// One column whose representation would shrink.
#[derive(Debug, Clone, Serialize)]
pub struct Suggestion {
    /// `blobs/{service}/{locator}/columns/{column}/suggestions/{rule}`.
    pub name: String,
    /// The stable rule id.
    pub rule: &'static str,
    /// How serious the suggestion is.
    pub severity: &'static str,
    /// The column's dotted leaf path.
    pub column: String,
    /// One-line explanation, with the measured bytes per value.
    pub message: String,
    /// The change that recovers the bytes.
    pub remedy: &'static str,
    /// Estimated bytes saved by the change.
    pub savings_bytes: u64,
    /// How the suggestion was established: `footer`, `sample`, or both.
    pub basis: &'static str,
    /// Rows decoded to establish the suggestion.
    pub sample_rows: u64,
}

/// One file's suggestion total.
#[derive(Debug, Clone, Serialize)]
pub struct SuggestionReport {
    /// `blobs/{service}/{locator}/suggestions`.
    pub name: String,
    /// The file's size in bytes.
    pub size_bytes: u64,
    /// Suggestions emitted for the file.
    pub suggestion_count: u64,
    /// Suggestions with a byte estimate.
    pub priced_count: u64,
    /// Sum of the suggestions' estimates.
    pub potential_savings_bytes: u64,
    /// [`Self::potential_savings_bytes`] divided by [`Self::size_bytes`].
    pub potential_savings_fraction: f64,
    /// Rows decoded for the file.
    pub sample_rows: u64,
}

/// One linted file: its suggestions and total.
#[derive(Debug, Clone)]
pub struct SuggestionsFile {
    pub suggestions: Vec<Suggestion>,
    pub report: SuggestionReport,
}

/// Errors from the lint layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The parquet layer failed.
    Parquet(ParquetError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parquet(_) => write!(f, "the parquet layer failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parquet(error) => Some(error),
        }
    }
}

impl From<ParquetError> for Error {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}

/// Lint one local Parquet file.
///
/// `blob` is the record's blob name, so each suggestion names the file the walk
/// read, not the local path a caller may have copied it to.
///
/// # Errors
/// Fails when the footer, the sample, or a binary sample encode cannot be read.
pub fn suggestions(
    path: &Path,
    blob: &str,
    sample_rows: Option<usize>,
) -> Result<SuggestionsFile, Error> {
    let mass = read_file_masses(path)?;
    let facts = fold_columns(mass.columns);
    let targets: Vec<String> = facts
        .iter()
        .filter(|(_, column)| !column.repeated && byte_leaf(&column.physical_data_type))
        .map(|(column, _)| column.clone())
        .collect();
    let sample = read_leaf_sample(path, &targets, sample_rows)?;
    let mut suggestions = Vec::new();

    for (at, column) in targets.iter().enumerate() {
        let fact = &facts[column];
        let cells: Vec<Option<&[u8]>> = sample
            .rows
            .iter()
            .map(|row| row.get(at).and_then(Option::as_deref))
            .collect();
        let non_null = cells.iter().filter(|cell| cell.is_some()).count() as u64;
        if non_null == 0 {
            continue;
        }
        let mut uuid = 0_u64;
        let mut versions = 0_u64;
        let mut variants = 0_u64;
        let mut binary_rows = Vec::with_capacity(cells.len());
        for cell in &cells {
            match cell.and_then(parse_uuid) {
                Some(bytes) => {
                    uuid += 1;
                    if (1..=8).contains(&(bytes[6] >> 4)) {
                        versions += 1;
                    }
                    if (8..=11).contains(&(bytes[8] >> 4)) {
                        variants += 1;
                    }
                    binary_rows.push(vec![Value::Bytes(bytes.to_vec())]);
                }
                None => binary_rows.push(vec![Value::Null]),
            }
        }
        if (uuid as f64 / non_null as f64) < UUID_FRACTION {
            continue;
        }
        let non_null_values = match fact.null_count {
            Some(nulls) => fact.value_count.saturating_sub(nulls),
            None => {
                let fraction = non_null as f64 / cells.len().max(1) as f64;
                (fact.value_count as f64 * fraction).round() as u64
            }
        };
        if non_null_values == 0 {
            continue;
        }
        let binary = write_parquet(
            &[TypedColumn {
                name: "uuid".into(),
                kind: Kind::Bytes,
            }],
            &binary_rows,
            &write_options(&fact.codec),
        )?;
        let binary_mass = read_buffer_masses(&binary)?;
        let Some(binary_bytes) = binary_mass
            .columns
            .first()
            .map(|column| column.compressed_bytes)
        else {
            continue;
        };
        let text_bpv = fact.compressed_bytes as f64 / non_null_values as f64;
        let binary_bpv = binary_bytes as f64 / non_null as f64;
        if binary_bpv >= text_bpv {
            continue;
        }
        let savings = ((text_bpv - binary_bpv) * non_null_values as f64).round() as u64;
        if savings < MIN_SAVINGS_BYTES {
            continue;
        }
        let annotated = versions == uuid && variants == uuid;
        suggestions.push(Suggestion {
            name: v1::name::suggestion(blob, column, UUID_TEXT_RULE),
            rule: UUID_TEXT_RULE,
            severity: "warning",
            column: column.clone(),
            message: format!(
                "UUID text at {text_bpv:.2} B/value; a 16-byte binary sample prices at \
                 {binary_bpv:.2} B/value over {non_null_values} values"
            ),
            remedy: if annotated {
                "store as BINARY(16) with the UUID logical type; disable the dictionary"
            } else {
                "store as BINARY(16) without a UUID logical type; disable the dictionary"
            },
            savings_bytes: savings,
            basis: "footer+sample",
            sample_rows: sample.rows.len() as u64,
        });
    }

    suggestions.sort_by(|left, right| {
        right
            .savings_bytes
            .cmp(&left.savings_bytes)
            .then_with(|| left.column.cmp(&right.column))
    });
    let potential_savings_bytes = suggestions
        .iter()
        .map(|suggestion| suggestion.savings_bytes)
        .sum();
    let report = SuggestionReport {
        name: v1::name::suggestions(blob),
        size_bytes: mass.size_bytes,
        suggestion_count: suggestions.len() as u64,
        priced_count: suggestions.len() as u64,
        potential_savings_bytes,
        potential_savings_fraction: if mass.size_bytes == 0 {
            0.0
        } else {
            potential_savings_bytes as f64 / mass.size_bytes as f64
        },
        sample_rows: sample.rows.len() as u64,
    };
    Ok(SuggestionsFile {
        suggestions,
        report,
    })
}

/// One column, folded over its row-group chunks.
struct ColumnFacts {
    physical_data_type: String,
    compressed_bytes: u64,
    value_count: u64,
    null_count: Option<u64>,
    null_stats: bool,
    repeated: bool,
    codec: String,
}

/// Fold one `ColumnMass` per chunk into one fact per column path.
fn fold_columns(chunks: Vec<ColumnMass>) -> BTreeMap<String, ColumnFacts> {
    let mut columns: BTreeMap<String, ColumnFacts> = BTreeMap::new();
    for chunk in chunks {
        let column = columns.entry(chunk.column).or_insert_with(|| ColumnFacts {
            physical_data_type: chunk.physical_data_type.clone(),
            compressed_bytes: 0,
            value_count: 0,
            null_count: Some(0),
            null_stats: true,
            repeated: chunk.max_repetition_level > 0,
            codec: chunk.codec.clone(),
        });
        column.compressed_bytes = column
            .compressed_bytes
            .saturating_add(chunk.compressed_bytes);
        column.value_count = column.value_count.saturating_add(chunk.num_values);
        column.repeated |= chunk.max_repetition_level > 0;
        match (column.null_stats, chunk.null_count) {
            (true, Some(count)) => {
                column.null_count = Some(column.null_count.unwrap_or(0).saturating_add(count));
            }
            (_, _) => {
                column.null_stats = false;
                column.null_count = None;
            }
        }
    }
    columns
}

/// Whether a physical type is a byte array the UUID rule can inspect.
fn byte_leaf(physical_data_type: &str) -> bool {
    matches!(physical_data_type, "BYTE_ARRAY" | "FIXED_LEN_BYTE_ARRAY")
}

/// Parse canonical 36-byte UUID text into its 16 big-endian bytes.
fn parse_uuid(bytes: &[u8]) -> Option<[u8; 16]> {
    if bytes.len() != 36 {
        return None;
    }
    let mut out = [0_u8; 16];
    let mut at = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if *byte != b'-' {
                return None;
            }
            continue;
        }
        let value = (*byte as char).to_digit(16)?;
        if at % 2 == 0 {
            out[at / 2] = (value as u8) << 4;
        } else {
            out[at / 2] |= value as u8;
        }
        at += 1;
    }
    (at == 32).then_some(out)
}

/// The writer settings used to price a binary rewrite: the file's codec, no
/// dictionary, and one row group.
fn write_options(codec: &str) -> WriteOptions {
    let codec = codec_name(codec);
    WriteOptions {
        compression: codec.clone(),
        level: (codec == "zstd").then_some(3),
        dictionary: false,
        dictionary_bytes: None,
        encoding: None,
        page_size: None,
        sorting_columns: Vec::new(),
        row_group_size: 0,
    }
}

/// The bare codec name a footer spells: parquet-rs renders `ZSTD(ZSTD(1))`, and
/// the writer takes `zstd`.
fn codec_name(codec: &str) -> String {
    let rendered = codec.to_lowercase();
    let head = rendered.split('(').next().unwrap_or(&rendered).trim();
    match head {
        "uncompressed" | "none" => "uncompressed",
        "snappy" => "snappy",
        "gzip" => "gzip",
        "lz4" | "lz4_raw" => "lz4",
        "zstd" => "zstd",
        other => other,
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::parse_uuid;

    #[test]
    fn canonical_uuid_text_parses_to_big_endian_bytes() {
        let bytes = parse_uuid(b"00112233-4455-6677-8899-aabbccddeeff").expect("uuid");
        assert_eq!(
            bytes,
            [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]
        );
    }

    #[test]
    fn malformed_uuid_text_is_rejected() {
        assert!(parse_uuid(b"00112233445566778899aabbccddeeff00").is_none());
        assert!(parse_uuid(b"00112233-4455-6677-8899-aabbccddeefg").is_none());
        assert!(parse_uuid(b"00112233-4455-6677-8899-aabbccddeef").is_none());
    }
}
