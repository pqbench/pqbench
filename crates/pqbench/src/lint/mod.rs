//! `lint`: the storage a column's values do not need.
//!
//! `list suggestions` emits the rewrites a column plausibly allows; `get
//! suggestion` measures each against a control written by the same writer at
//! the same codec and level and marks it verified or not. A rule is one module
//! with a `detect` that reads only the source and a `measure` that runs the
//! controlled pair. The first rule that detects a column wins, so a column's
//! bytes are never counted twice.
//!
//! A suggestion carries the file's writer, because its remedy depends on what
//! that writer can change: parquet-mr, pyarrow, and parquet-rs take
//! per-column options, while Photon forwards them unverified.

mod control;
mod dictionary;
mod price;
mod uuid;
mod writer;

use std::fmt;
use std::path::Path;

use crate::third_party::parquet::api::{read_leaf_sample, Error as ParquetError, Value};
use crate::v1;

use self::control::Measurement;
use self::writer::Writer;

pub use dictionary::DICTIONARY_UNIQUE_RULE;
pub use uuid::UUID_TEXT_RULE;

/// Leading rows decoded to judge a column's values and price the rewrite.
pub const SAMPLE_ROWS: usize = 8192;

/// Wins smaller than this are noted as noise, not dropped.
const NOISE_FLOOR_BYTES: u64 = 64 * 1024;

/// Errors from the lint layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The parquet layer failed.
    Parquet(ParquetError),
    /// A suggestion named a rule this build does not have.
    Rule(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parquet(_) => write!(f, "the parquet layer failed"),
            Self::Rule(rule) => write!(f, "unknown suggestion rule `{rule}`"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parquet(error) => Some(error),
            Self::Rule(_) => None,
        }
    }
}

impl From<ParquetError> for Error {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}

/// One measured column and its decoded sample: what a rule detects and
/// measures.
pub(crate) struct ColumnSample<'a> {
    blob: &'a str,
    leaf: &'a str,
    column: &'a v1::Column,
    mass: &'a v1::Bytemass,
    cells: Vec<Option<&'a [u8]>>,
    writer: Option<&'a str>,
    sample_rows: usize,
    text_bpv: f64,
}

impl<'a> ColumnSample<'a> {
    /// The sample a rule reads, or `None` when the footer lacks the counts or
    /// the sample has no non-null value.
    fn new(
        blob: &'a str,
        leaf: &'a str,
        column: &'a v1::Column,
        cells: Vec<Option<&'a [u8]>>,
        writer: Option<&'a str>,
        sample_rows: usize,
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
            writer,
            sample_rows,
            text_bpv: compressed_bytes as f64 / non_null_values as f64,
        })
    }

    /// The values as one nullable byte column: the control's text arm.
    fn text_rows(&self) -> Vec<Vec<Value>> {
        self.cells
            .iter()
            .map(|cell| {
                vec![match *cell {
                    Some(bytes) => Value::Bytes(bytes.to_vec()),
                    None => Value::Null,
                }]
            })
            .collect()
    }

    /// The writer this file came from, as the rules' remedies need it.
    fn writer_kind(&self) -> Writer {
        Writer::classify(self.writer)
    }

    /// One potential suggestion, on the column's own name and context.
    fn candidate(&self, rule: &'static str, finding: Finding) -> v1::Suggestion {
        v1::Suggestion {
            name: v1::name::suggestion(self.blob, self.leaf, rule),
            rule: rule.to_string(),
            column: self.leaf.to_string(),
            table: self.column.table.clone(),
            sample_rate: self.column.sample_rate,
            size_bytes: self.column.size_bytes,
            writer: self.writer.map(ToString::to_string),
            message: finding.message,
            remedy: finding.remedy,
            verified: None,
            savings_bytes: None,
            sample_row_count: Some(self.sample_rows as u64),
        }
    }
}

/// What a rule found and how to measure its rewrite.
pub(crate) struct Finding {
    pub(crate) message: String,
    pub(crate) remedy: String,
    pub(crate) measure: Measure,
}

/// A rule's controlled measurement of its rewrite.
pub(crate) type Measure = fn(&ColumnSample<'_>) -> Result<Option<Measurement>, Error>;

/// A rule: the detection that makes a column a candidate, and the control that
/// verifies it.
pub(crate) struct Rule {
    pub(crate) id: &'static str,
    pub(crate) detect: fn(&ColumnSample<'_>) -> Option<Finding>,
}

/// The rules, in priority order. The first that detects a column wins, so a
/// column's bytes are priced once and two rules never count the same saving.
pub(crate) const RULES: &[Rule] = &[
    Rule {
        id: UUID_TEXT_RULE,
        detect: uuid::detect,
    },
    Rule {
        id: DICTIONARY_UNIQUE_RULE,
        detect: dictionary::detect,
    },
];

/// The rule with this id, when this build has it.
fn rule(id: &str) -> Option<&'static Rule> {
    RULES.iter().find(|rule| rule.id == id)
}

/// Lint one local Parquet file's measured columns into potential suggestions.
///
/// `blob` is the file's record, so each suggestion names the file the walk
/// read, not the local path a caller may have copied it to. No rewrite is
/// encoded here: `verify` does that.
///
/// # Errors
/// Fails when the sample cannot be read.
pub fn suggestions(
    path: &Path,
    blob: &v1::Blob,
    columns: &[v1::Column],
    sample_rows: Option<usize>,
) -> Result<Vec<v1::Suggestion>, Error> {
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
    let rows = sample_rows.unwrap_or(SAMPLE_ROWS);
    let leaves: Vec<String> = targets.iter().map(|(_, leaf)| leaf.clone()).collect();
    let decoded = read_leaf_sample(path, &leaves, Some(rows))?;
    let mut suggestions = Vec::new();

    for (at, (column, leaf)) in targets.iter().enumerate() {
        let cells: Vec<Option<&[u8]>> = decoded[at].iter().map(Option::as_deref).collect();
        let Some(sample) = ColumnSample::new(
            &blob.name,
            leaf,
            column,
            cells,
            blob.writer.as_deref(),
            rows,
        ) else {
            continue;
        };
        let compressed = column
            .mass
            .as_ref()
            .and_then(|mass| mass.compressed_bytes)
            .unwrap_or(0);
        for rule in RULES {
            if let Some(finding) = (rule.detect)(&sample) {
                suggestions.push((compressed, sample.candidate(rule.id, finding)));
                break;
            }
        }
    }

    suggestions.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.column.cmp(&right.1.column))
    });
    Ok(suggestions
        .into_iter()
        .map(|(_, suggestion)| suggestion)
        .collect())
}

/// Measure one potential suggestion against its control.
///
/// The sample size is the caller's, else the record's `sample_row_count`, else
/// [`SAMPLE_ROWS`]; so `list | get` measures the values `list` detected.
///
/// # Errors
/// Fails when the sample cannot be read or the rule is unknown.
pub fn verify(
    path: &Path,
    blob: &v1::Blob,
    column: &v1::Column,
    candidate: &v1::Suggestion,
    sample_rows: Option<usize>,
) -> Result<v1::Suggestion, Error> {
    let rows = sample_rows
        .or_else(|| {
            candidate
                .sample_row_count
                .and_then(|count| usize::try_from(count).ok())
        })
        .unwrap_or(SAMPLE_ROWS);
    if column.max_repetition_level != Some(0) {
        return Ok(reject(
            candidate,
            blob,
            rows,
            "the column is repeated; suggestions measure scalar leaves only",
        ));
    }
    let leaf = candidate.column.clone();
    let mut decoded = read_leaf_sample(path, std::slice::from_ref(&leaf), Some(rows))?;
    let cells = decoded.pop().unwrap_or_default();
    let cells: Vec<Option<&[u8]>> = cells.iter().map(Option::as_deref).collect();
    let Some(sample) = ColumnSample::new(
        &blob.name,
        &leaf,
        column,
        cells,
        blob.writer.as_deref(),
        rows,
    ) else {
        return Ok(reject(
            candidate,
            blob,
            rows,
            "the sample has no non-null value",
        ));
    };
    let Some(rule) = rule(&candidate.rule) else {
        return Err(Error::Rule(candidate.rule.clone()));
    };
    let Some(finding) = (rule.detect)(&sample) else {
        return Ok(reject(
            candidate,
            blob,
            rows,
            "the rule no longer detects this column",
        ));
    };
    let measurement = (finding.measure)(&sample)?;
    let compressed = column
        .mass
        .as_ref()
        .and_then(|mass| mass.compressed_bytes)
        .unwrap_or(0);
    Ok(enriched(
        candidate,
        blob,
        rows,
        finding,
        measurement,
        compressed,
    ))
}

/// A candidate the control could not confirm as it stands.
fn reject(
    candidate: &v1::Suggestion,
    blob: &v1::Blob,
    rows: usize,
    reason: &str,
) -> v1::Suggestion {
    let mut rejected = candidate.clone();
    rejected.writer = blob.writer.clone();
    rejected.size_bytes = blob.size_bytes;
    rejected.sample_row_count = Some(rows as u64);
    rejected.verified = Some(false);
    rejected.savings_bytes = None;
    rejected.message = format!("{}; {reason}", candidate.message);
    rejected
}

/// A candidate the control measured.
fn enriched(
    candidate: &v1::Suggestion,
    blob: &v1::Blob,
    rows: usize,
    finding: Finding,
    measurement: Option<Measurement>,
    compressed_bytes: u64,
) -> v1::Suggestion {
    let mut enriched = candidate.clone();
    enriched.writer = blob.writer.clone();
    enriched.size_bytes = blob.size_bytes;
    enriched.sample_row_count = Some(rows as u64);
    enriched.remedy = finding.remedy;
    let Some(measurement) = measurement else {
        enriched.verified = None;
        enriched.savings_bytes = None;
        enriched.message = format!(
            "{}; not measurable at this sample: the writer cannot reproduce the file's codec",
            finding.message
        );
        return enriched;
    };
    let saving = (compressed_bytes as f64 * measurement.ratio_low)
        .round()
        .max(0.0) as u64;
    let verified = measurement.ratio_low > 0.0 && saving > 0;
    enriched.verified = Some(verified);
    enriched.savings_bytes = verified.then_some(saving);
    enriched.message = format!(
        "{}; {}",
        finding.message,
        verdict(&measurement, saving, verified)
    );
    enriched
}

/// The control's result in words.
fn verdict(measurement: &Measurement, saving: u64, verified: bool) -> String {
    let low = measurement.ratio_low * 100.0;
    let high = measurement.ratio_high * 100.0;
    if verified {
        let smaller = if (high - low).abs() < 0.1 {
            format!(
                "the controlled rewrite is {}% smaller at {}",
                percent(low),
                measurement.levels
            )
        } else {
            format!(
                "the controlled rewrite is {}-{}% smaller at {}",
                percent(low),
                percent(high),
                measurement.levels
            )
        };
        if saving < NOISE_FLOOR_BYTES {
            format!("{smaller}; {saving} bytes is below the 64 KiB noise floor")
        } else {
            smaller
        }
    } else if measurement.ratio_high <= 0.0 {
        format!(
            "the controlled rewrite is not smaller at {} (best {high:+.1}%)",
            measurement.levels
        )
    } else if measurement.ratio_low <= 0.0 {
        format!(
            "the controlled rewrite wins only at some {} levels (best {}%)",
            measurement.levels,
            percent(high)
        )
    } else {
        format!("the controlled rewrite saves only {saving} bytes, below the 64 KiB noise floor")
    }
}

/// A percentage with enough precision to show a small win.
fn percent(value: f64) -> String {
    if value < 0.1 {
        "<0.1".to_string()
    } else if value < 10.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.0}")
    }
}
