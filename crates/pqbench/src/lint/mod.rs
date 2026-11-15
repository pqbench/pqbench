//! `lint`: the storage a column's values do not need.
//!
//! Each rule prices one rewrite of a sampled column and returns at most one
//! suggestion: UUID text as 16-byte binary, a dictionary page on nearly-unique
//! values with the dictionary off. The footer gives the column's on-disk
//! bytes; a decoded sample gives its value shape and lets the rule encode the
//! same values the other way. A suggestion is emitted only when that encode is
//! smaller, so repeated UUIDs price below the 16-byte floor they would have if
//! every value were unique.
//!
//! A new rule is a module with a `suggest` function plus one entry in
//! [`RULES`]. The first rule that fires wins, so a column's bytes are priced
//! once and two rules never count the same saving.

mod dictionary;
mod price;
mod uuid;

use std::fmt;
use std::path::Path;

use serde::Serialize;

use crate::third_party::parquet::api::{read_leaf_sample, Error as ParquetError, WriteOptions};
use crate::v1;

pub use dictionary::DICTIONARY_UNIQUE_RULE;
pub use uuid::UUID_TEXT_RULE;

/// Leading rows decoded to judge a column's values and price the rewrite.
pub const SAMPLE_ROWS: usize = 8192;

/// Suggestions smaller than this are noise.
const MIN_SAVINGS_BYTES: u64 = 64 * 1024;

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

/// One measured column and its decoded sample: what a rule prices.
pub(crate) struct ColumnSample<'a> {
    blob: &'a str,
    leaf: &'a str,
    column: &'a v1::Column,
    mass: &'a v1::Bytemass,
    cells: Vec<Option<&'a [u8]>>,
    writer: WriteOptions,
    non_null: u64,
    non_null_values: u64,
    text_bpv: f64,
}

impl<'a> ColumnSample<'a> {
    /// The sample a rule prices, or `None` when the footer lacks the counts or
    /// the sample has no non-null value.
    fn new(
        blob: &'a str,
        leaf: &'a str,
        column: &'a v1::Column,
        cells: Vec<Option<&'a [u8]>>,
    ) -> Option<Self> {
        let mass = column.mass.as_ref()?;
        let compressed_bytes = mass.compressed_bytes?;
        let value_count = mass.value_count?;
        let non_null = cells.iter().filter(|cell| cell.is_some()).count() as u64;
        if non_null == 0 {
            return None;
        }
        let non_null_values = match mass.null_count {
            Some(nulls) => value_count.saturating_sub(nulls),
            None => {
                let fraction = non_null as f64 / cells.len().max(1) as f64;
                (value_count as f64 * fraction).round() as u64
            }
        };
        if non_null_values == 0 {
            return None;
        }
        Some(Self {
            blob,
            leaf,
            column,
            mass,
            cells,
            writer: price::write_options(mass.codec.as_deref()),
            non_null,
            non_null_values,
            text_bpv: compressed_bytes as f64 / non_null_values as f64,
        })
    }

    /// The bytes a rewrite saves, when `priced_bpv` beats the on-disk rate by
    /// at least the noise floor.
    fn saving(&self, priced_bpv: f64) -> Option<u64> {
        if priced_bpv >= self.text_bpv {
            return None;
        }
        let savings = ((self.text_bpv - priced_bpv) * self.non_null_values as f64).round() as u64;
        (savings >= MIN_SAVINGS_BYTES).then_some(savings)
    }

    /// One suggestion record, on the column's own name and context.
    fn suggestion(
        &self,
        rule: &'static str,
        message: String,
        remedy: &'static str,
        savings_bytes: u64,
    ) -> Suggestion {
        Suggestion {
            name: v1::name::suggestion(self.blob, self.leaf, rule),
            rule,
            column: self.leaf.to_string(),
            table: self.column.table.clone(),
            sample_rate: self.column.sample_rate,
            size_bytes: self.column.size_bytes,
            message,
            remedy,
            savings_bytes,
        }
    }
}

/// A rule: the one suggestion a sampled column earns, if any.
type Rule = fn(&ColumnSample<'_>) -> Result<Option<Suggestion>, Error>;

/// The rules, in priority order. The first that fires wins, so a column's
/// bytes are priced once and two rules never count the same saving.
const RULES: &[Rule] = &[uuid::suggest, dictionary::suggest];

/// Lint one local Parquet file's measured columns.
///
/// `blob` is the record's blob name, so each suggestion names the file the walk
/// read, not the local path a caller may have copied it to.
///
/// # Errors
/// Fails when the sample or a rewrite encode cannot be read.
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
    let decoded = read_leaf_sample(path, &leaves, sample_rows)?;
    let mut suggestions = Vec::new();

    for (at, (column, leaf)) in targets.iter().enumerate() {
        let cells: Vec<Option<&[u8]>> = decoded[at].iter().map(Option::as_deref).collect();
        let Some(sample) = ColumnSample::new(blob, leaf, column, cells) else {
            continue;
        };
        for rule in RULES {
            if let Some(suggestion) = rule(&sample)? {
                suggestions.push(suggestion);
                break;
            }
        }
    }

    suggestions.sort_by(|left, right| {
        right
            .savings_bytes
            .cmp(&left.savings_bytes)
            .then_with(|| left.column.cmp(&right.column))
    });
    Ok(suggestions)
}
