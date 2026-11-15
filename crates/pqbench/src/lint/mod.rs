//! `lint`: the storage a column's values do not need.
//!
//! The first rule prices UUID text stored as 36-byte strings. The footer gives
//! the column's on-disk bytes; a decoded sample gives its value shape and lets
//! the rule encode the same values as 16-byte binary. A suggestion is emitted
//! only when that binary sample is smaller, so repeated UUIDs are priced below
//! the 16-byte floor they would have if every value were unique.

use std::fmt;
use std::path::Path;

use serde::Serialize;

use crate::third_party::parquet::api::{
    read_buffer_masses, read_leaf_sample, write_parquet, Error as ParquetError, Kind, TypedColumn,
    Value, WriteOptions,
};
use crate::v1;

/// Leading rows decoded to judge a column's values and price the rewrite.
pub const SAMPLE_ROWS: usize = 8192;

/// Suggestions smaller than this are noise.
const MIN_SAVINGS_BYTES: u64 = 64 * 1024;

/// The rule id a suggestion carries.
pub const UUID_TEXT_RULE: &str = "uuid-text";

/// One column whose representation would shrink.
#[derive(Debug, Clone, Serialize)]
pub struct Suggestion {
    /// `{blob}/columns/{column}/suggestions/{rule}`.
    pub name: String,
    /// The stable rule id.
    pub rule: &'static str,
    /// The column's dotted leaf path.
    pub column: String,
    /// The table whose file was measured, when measured through a table file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// The probability the table file was kept with, when a sample stage chose it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<f64>,
    /// The size in bytes of the file the column was measured in, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// One-line explanation, with the measured bytes per value.
    pub message: String,
    /// The change that recovers the bytes.
    pub remedy: &'static str,
    /// Estimated bytes saved by the change.
    pub savings_bytes: u64,
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

/// Lint one local Parquet file's measured columns.
///
/// `blob` is the record's blob name, so each suggestion names the file the walk
/// read, not the local path a caller may have copied it to.
///
/// # Errors
/// Fails when the sample or a binary sample encode cannot be read.
pub fn suggestions(
    path: &Path,
    blob: &str,
    columns: &[v1::Column],
    sample_rows: Option<usize>,
) -> Result<Vec<Suggestion>, Error> {
    let targets: Vec<(&v1::Column, String)> = columns
        .iter()
        .filter(|column| {
            column.max_repetition_level == Some(0)
                && matches!(
                    column.physical_data_type.as_deref(),
                    Some("BYTE_ARRAY" | "FIXED_LEN_BYTE_ARRAY")
                )
        })
        .filter_map(|column| {
            let (_, _, leaf) = v1::Column::parse_name(&column.name)?;
            Some((column, leaf))
        })
        .collect();
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let leaves: Vec<String> = targets.iter().map(|(_, leaf)| leaf.clone()).collect();
    let sample = read_leaf_sample(path, &leaves, sample_rows)?;
    let mut suggestions = Vec::new();

    for (at, (column, leaf)) in targets.iter().enumerate() {
        let cells: Vec<Option<&[u8]>> = sample[at].iter().map(Option::as_deref).collect();
        let uuids: Option<Vec<Option<[u8; 16]>>> = cells
            .iter()
            .map(|cell| match *cell {
                Some(bytes) => parse_uuid(bytes).map(Some),
                None => Some(None),
            })
            .collect();
        let Some(uuids) = uuids else { continue };
        let non_null = uuids.iter().filter(|uuid| uuid.is_some()).count() as u64;
        if non_null == 0 {
            continue;
        }
        let Some(mass) = column.mass.as_ref() else {
            continue;
        };
        let Some(compressed_bytes) = mass.compressed_bytes else {
            continue;
        };
        let Some(value_count) = mass.value_count else {
            continue;
        };
        let non_null_values = match mass.null_count {
            Some(nulls) => value_count.saturating_sub(nulls),
            None => {
                let fraction = non_null as f64 / cells.len().max(1) as f64;
                (value_count as f64 * fraction).round() as u64
            }
        };
        if non_null_values == 0 {
            continue;
        }
        let binary_rows: Vec<Vec<Value>> = uuids
            .iter()
            .map(|uuid| {
                vec![match uuid {
                    Some(bytes) => Value::Bytes(bytes.to_vec()),
                    None => Value::Null,
                }]
            })
            .collect();
        let binary = write_parquet(
            &[TypedColumn {
                name: "uuid".into(),
                kind: Kind::Bytes,
            }],
            &binary_rows,
            &write_options(mass.codec.as_deref()),
        )?;
        let binary_mass = read_buffer_masses(&binary)?;
        let Some(binary_bytes) = binary_mass
            .columns
            .first()
            .map(|column| column.compressed_bytes)
        else {
            continue;
        };
        let text_bpv = compressed_bytes as f64 / non_null_values as f64;
        let binary_bpv = binary_bytes as f64 / non_null as f64;
        if binary_bpv >= text_bpv {
            continue;
        }
        let savings = ((text_bpv - binary_bpv) * non_null_values as f64).round() as u64;
        if savings < MIN_SAVINGS_BYTES {
            continue;
        }
        let annotated = uuids
            .iter()
            .flatten()
            .all(|bytes| (1..=8).contains(&(bytes[6] >> 4)) && (8..=11).contains(&(bytes[8] >> 4)));
        suggestions.push(Suggestion {
            name: v1::name::suggestion(blob, leaf, UUID_TEXT_RULE),
            rule: UUID_TEXT_RULE,
            column: leaf.clone(),
            table: column.table.clone(),
            sample_rate: column.sample_rate,
            size_bytes: column.size_bytes,
            message: format!(
                "UUID text at {text_bpv:.2} B/value; a 16-byte binary sample prices at \
                 {binary_bpv:.2} B/value"
            ),
            remedy: if annotated {
                "store as BINARY(16) with the UUID logical type"
            } else {
                "store as BINARY(16)"
            },
            savings_bytes: savings,
        });
    }

    suggestions.sort_by(|left, right| {
        right
            .savings_bytes
            .cmp(&left.savings_bytes)
            .then_with(|| left.column.cmp(&right.column))
    });
    Ok(suggestions)
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

/// The writer settings used to price a binary rewrite: the file's codec when
/// the writer has it (zstd otherwise), no dictionary, and one row group.
fn write_options(codec: Option<&str>) -> WriteOptions {
    let rendered = codec.unwrap_or("zstd").to_lowercase();
    let codec = match rendered.split('(').next().unwrap_or("zstd").trim() {
        "uncompressed" | "none" => "uncompressed",
        "snappy" => "snappy",
        "gzip" => "gzip",
        "lz4" | "lz4_raw" => "lz4",
        "zstd" => "zstd",
        _ => "zstd",
    };
    WriteOptions {
        compression: codec.to_string(),
        level: (codec == "zstd").then_some(3),
        dictionary: false,
        dictionary_bytes: None,
        encoding: None,
        page_size: None,
        sorting_columns: Vec::new(),
        row_group_size: 0,
    }
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
