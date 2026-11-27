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

use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::{ByteArray, DataType};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, RowGroupReader, SerializedFileReader};

use crate::third_party::parquet::api::Error as ParquetError;
use crate::v1;

use self::control::Measurement;
use self::writer::Writer;

pub use dictionary::DICTIONARY_UNIQUE_RULE;
pub use uuid::UUID_TEXT_RULE;

/// Wins smaller than this are noted as noise, not dropped.
const NOISE_FLOOR_BYTES: u64 = 64 * 1024;

/// Leading whole row groups are decoded until the target columns' compressed
/// bytes reach this limit; at least one group is always decoded.
const SAMPLE_BYTE_LIMIT: u64 = 8 * 1024 * 1024;

/// Errors from the lint layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The parquet layer failed.
    Parquet(ParquetError),
    /// A suggestion named a rule this build does not have.
    Rule(String),
    /// The input record was invalid or did not match the file.
    Invalid(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parquet(_) => write!(f, "the parquet layer failed"),
            Self::Rule(rule) => write!(f, "unknown suggestion rule `{rule}`"),
            Self::Invalid(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parquet(error) => Some(error),
            Self::Rule(_) | Self::Invalid(_) => None,
        }
    }
}

impl From<ParquetError> for Error {
    fn from(error: ParquetError) -> Self {
        Self::Parquet(error)
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Self::Parquet(ParquetError::Parquet(Box::new(error)))
    }
}

impl From<crate::bytemass::Error> for Error {
    fn from(error: crate::bytemass::Error) -> Self {
        Self::Invalid(error.to_string())
    }
}

/// What a sample decoded: rows and the whole row groups they came from.
#[derive(Clone, Copy, Default)]
struct Coverage {
    row_count: u64,
    row_group_count: u64,
}

/// Decoded whole-row-group chunks for the requested scalar leaf columns.
struct DecodedLeaves {
    /// Per requested column: one `Vec<Option<ByteArray>>` per sampled row group.
    columns: Vec<Vec<Vec<Option<ByteArray>>>>,
    /// Compressed bytes of each requested column across the sampled row groups.
    compressed_bytes: Vec<u64>,
    coverage: Coverage,
}

/// One measured column and its decoded sample: what a rule detects and
/// measures.
pub(crate) struct ColumnSample<'a> {
    blob: &'a str,
    leaf: &'a str,
    column: &'a v1::Column,
    mass: &'a v1::Bytemass,
    /// One slice per sampled row group, preserving chunk boundaries.
    groups: &'a [Vec<Option<ByteArray>>],
    writer: Option<&'a str>,
    coverage: Coverage,
    text_bpv: f64,
}

impl<'a> ColumnSample<'a> {
    /// The sample a rule reads, or `None` when the footer lacks the counts or
    /// the sample has no non-null value.
    fn new(
        blob: &'a str,
        leaf: &'a str,
        column: &'a v1::Column,
        groups: &'a [Vec<Option<ByteArray>>],
        sampled_compressed_bytes: u64,
        writer: Option<&'a str>,
        coverage: Coverage,
    ) -> Option<Self> {
        let mass = column.mass.as_ref()?;
        let non_null: u64 = groups
            .iter()
            .flat_map(|cells| cells.iter())
            .filter(|cell| cell.is_some())
            .count() as u64;
        if non_null == 0 {
            return None;
        }
        let text_bpv = if sampled_compressed_bytes > 0 {
            sampled_compressed_bytes as f64 / non_null as f64
        } else {
            let compressed_bytes = mass.compressed_bytes?;
            let value_count = mass.value_count?;
            let non_null_values = match mass.null_count {
                Some(nulls) => value_count.saturating_sub(nulls),
                None => {
                    let total: usize = groups.iter().map(Vec::len).sum();
                    let fraction = non_null as f64 / total.max(1) as f64;
                    (value_count as f64 * fraction).round() as u64
                }
            };
            if non_null_values == 0 {
                return None;
            }
            compressed_bytes as f64 / non_null_values as f64
        };
        Some(Self {
            blob,
            leaf,
            column,
            mass,
            groups,
            writer,
            coverage,
            text_bpv,
        })
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
            sample_row_count: Some(self.coverage.row_count),
            sample_row_group_count: Some(self.coverage.row_group_count),
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

/// Lint one local Parquet file into potential suggestions.
///
/// `file` carries the table file's identity and blob name, so each suggestion
/// names the blob the walk read, not the local path a caller may have copied it
/// to. No rewrite is encoded here: `verify` does that. The sample is whole
/// leading row groups, because a codec decision is made per column chunk.
///
/// # Errors
/// Fails when `file` has no blob name or the file cannot be read.
pub fn suggestions(path: &Path, file: &v1::TableFile) -> Result<Vec<v1::Suggestion>, Error> {
    let blob_name = file
        .blob
        .as_deref()
        .ok_or_else(|| Error::Invalid("a table-file record needs a blob to read".into()))?;
    let mut measured = crate::bytemass::measure_footer(blob_name, path)?;
    let table = v1::TableFile::parse_name(&file.name)
        .map(|(catalog, schema, table, _)| v1::name::table(&catalog, &schema, &table));
    for column in &mut measured.columns {
        column.table = table.clone();
        column.sample_rate = file.sample_rate;
        column.size_bytes = measured.blob.size_bytes;
    }
    let blob = &measured.blob;
    let columns = &measured.columns;
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
    let decoded = decode_leaves(path, &leaves, None)?;
    let coverage = decoded.coverage;
    let mut suggestions = Vec::new();

    for (at, (column, leaf)) in targets.iter().enumerate() {
        let Some(sample) = ColumnSample::new(
            &blob.name,
            leaf,
            column,
            &decoded.columns[at],
            decoded.compressed_bytes[at],
            blob.writer.as_deref(),
            coverage,
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
/// The sample is the leading row groups the candidate's
/// `sample_row_group_count` names, so `list | get` measures the same values
/// `list` decoded. The record then carries the rows actually read.
///
/// # Errors
/// Fails when the suggestion is invalid, the sample cannot be read, or the rule
/// is unknown.
pub fn verify(path: &Path, candidate: &v1::Suggestion) -> Result<v1::Suggestion, Error> {
    let (service, locator, column_name, rule_id) = v1::Suggestion::parse_name(&candidate.name)
        .ok_or_else(|| Error::Invalid(format!("not a suggestion name: {}", candidate.name)))?;
    if candidate.column != column_name || candidate.rule != rule_id {
        return Err(Error::Invalid(format!(
            "the suggestion name names `{column_name}` / `{rule_id}`, its fields say `{}` / `{}`",
            candidate.column, candidate.rule
        )));
    }
    let blob_name = v1::name::blob(&service, &locator);
    let measured = crate::bytemass::measure_footer(&blob_name, path)?;
    let blob = &measured.blob;
    let column = measured
        .columns
        .iter()
        .find(|measured| {
            v1::Column::parse_name(&measured.name)
                .is_some_and(|(_, _, leaf)| leaf == candidate.column)
        })
        .ok_or_else(|| Error::Invalid(format!("the file has no column `{}`", candidate.column)))?;
    if column.max_repetition_level != Some(0) {
        return Ok(reject(
            candidate,
            blob,
            None,
            "the column is repeated; suggestions measure scalar leaves only",
        ));
    }
    let leaf = candidate.column.clone();
    let row_groups = candidate
        .sample_row_group_count
        .and_then(|count| usize::try_from(count).ok());
    let decoded = decode_leaves(path, std::slice::from_ref(&leaf), row_groups)?;
    let coverage = decoded.coverage;
    let groups = decoded.columns.first().map_or(&[][..], Vec::as_slice);
    let sampled_bytes = decoded.compressed_bytes.first().copied().unwrap_or(0);
    let Some(sample) = ColumnSample::new(
        &blob.name,
        &leaf,
        column,
        groups,
        sampled_bytes,
        blob.writer.as_deref(),
        coverage,
    ) else {
        return Ok(reject(
            candidate,
            blob,
            Some(coverage),
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
            Some(coverage),
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
        coverage,
        finding,
        measurement,
        compressed,
    ))
}

/// A candidate the control could not confirm as it stands.
fn reject(
    candidate: &v1::Suggestion,
    blob: &v1::Blob,
    coverage: Option<Coverage>,
    reason: &str,
) -> v1::Suggestion {
    let mut rejected = candidate.clone();
    rejected.writer = blob.writer.clone();
    rejected.size_bytes = blob.size_bytes;
    if let Some(coverage) = coverage {
        rejected.sample_row_count = Some(coverage.row_count);
        rejected.sample_row_group_count = Some(coverage.row_group_count);
    }
    rejected.verified = Some(false);
    rejected.savings_bytes = None;
    rejected.message = format!("{}; {reason}", candidate.message);
    rejected
}

/// A candidate the control measured.
fn enriched(
    candidate: &v1::Suggestion,
    blob: &v1::Blob,
    coverage: Coverage,
    finding: Finding,
    measurement: Option<Measurement>,
    compressed_bytes: u64,
) -> v1::Suggestion {
    let mut enriched = candidate.clone();
    enriched.writer = blob.writer.clone();
    enriched.size_bytes = blob.size_bytes;
    enriched.sample_row_count = Some(coverage.row_count);
    enriched.sample_row_group_count = Some(coverage.row_group_count);
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

/// Decode whole leading row groups of the requested scalar byte leaf columns
/// directly from their column chunks; unselected columns are never decoded.
fn decode_leaves(
    path: &Path,
    leaves: &[String],
    row_groups: Option<usize>,
) -> Result<DecodedLeaves, Error> {
    let file = std::fs::File::open(path).map_err(ParquetError::Io)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();
    let leaf_index: HashMap<String, usize> = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, descr)| descr.max_rep_level() == 0)
        .map(|(at, descr)| (descr.path().string(), at))
        .collect();
    let targets: Vec<Option<usize>> = leaves
        .iter()
        .map(|name| leaf_index.get(name).copied())
        .collect();
    let target_indices: Vec<usize> = targets.iter().flatten().copied().collect();
    let sampled = sampled_row_groups(metadata, &target_indices, row_groups);

    let mut columns: Vec<Vec<Vec<Option<ByteArray>>>> = (0..leaves.len())
        .map(|_| Vec::with_capacity(sampled))
        .collect();
    let mut compressed_bytes = vec![0_u64; leaves.len()];
    let mut row_count = 0_u64;

    for rg in 0..sampled {
        let group = reader.get_row_group(rg)?;
        let group_rows = usize::try_from(group.metadata().num_rows()).unwrap_or(0);
        row_count += group_rows as u64;
        for (slot, col_idx) in targets.iter().enumerate() {
            let Some(col_idx) = *col_idx else {
                columns[slot].push(vec![None; group_rows]);
                continue;
            };
            let meta = group.metadata().column(col_idx);
            compressed_bytes[slot] += u64::try_from(meta.compressed_size()).unwrap_or(0);
            let max_def = schema.column(col_idx).max_def_level();
            let cells = decode_bytes_chunk(group.as_ref(), col_idx, max_def, group_rows)?;
            columns[slot].push(cells);
        }
    }

    Ok(DecodedLeaves {
        columns,
        compressed_bytes,
        coverage: Coverage {
            row_count,
            row_group_count: sampled as u64,
        },
    })
}

/// How many leading row groups to sample: `row_groups` when given, else whole
/// groups until the target columns' compressed bytes reach
/// [`SAMPLE_BYTE_LIMIT`] (always at least one group).
fn sampled_row_groups(
    metadata: &ParquetMetaData,
    target_indices: &[usize],
    row_groups: Option<usize>,
) -> usize {
    let available = metadata.num_row_groups();
    if available == 0 {
        return 0;
    }
    if let Some(count) = row_groups {
        return count.min(available);
    }
    let mut bytes = 0_u64;
    let mut count = 0;
    for group in metadata.row_groups() {
        let group_bytes: u64 = if target_indices.is_empty() {
            u64::try_from(group.compressed_size()).unwrap_or(0)
        } else {
            target_indices
                .iter()
                .map(|&col| u64::try_from(group.column(col).compressed_size()).unwrap_or(0))
                .sum()
        };
        if count > 0 && bytes + group_bytes > SAMPLE_BYTE_LIMIT {
            break;
        }
        bytes += group_bytes;
        count += 1;
    }
    count.max(1)
}

fn decode_bytes_chunk(
    group: &dyn RowGroupReader,
    col_idx: usize,
    max_def: i16,
    rows: usize,
) -> Result<Vec<Option<ByteArray>>, Error> {
    match group.get_column_reader(col_idx)? {
        ColumnReader::ByteArrayColumnReader(mut reader) => {
            decode_typed(&mut reader, max_def, rows, |value| value.clone())
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut reader) => {
            decode_typed(&mut reader, max_def, rows, |value| {
                ByteArray::from(value.data())
            })
        }
        _ => Ok(vec![None; rows]),
    }
}

fn decode_typed<T: DataType>(
    reader: &mut ColumnReaderImpl<T>,
    max_def: i16,
    rows: usize,
    mut to_bytes: impl FnMut(&T::T) -> ByteArray,
) -> Result<Vec<Option<ByteArray>>, Error> {
    let mut out = Vec::with_capacity(rows);
    let mut remaining = rows;
    let mut values = Vec::new();
    let mut def_levels = Vec::new();
    while remaining > 0 {
        let batch = remaining.min(1024);
        values.clear();
        def_levels.clear();
        let (_, _, levels) = reader.read_records(
            batch,
            (max_def > 0).then_some(&mut def_levels),
            None,
            &mut values,
        )?;
        let consumed = if max_def > 0 { levels } else { values.len() };
        if consumed == 0 {
            break;
        }
        if max_def == 0 {
            out.extend(values.iter().map(|v| Some(to_bytes(v))));
        } else {
            let mut at = 0;
            for &def in &def_levels[..levels] {
                if def == max_def {
                    out.push(Some(to_bytes(&values[at])));
                    at += 1;
                } else {
                    out.push(None);
                }
            }
        }
        remaining = remaining.saturating_sub(consumed);
    }
    Ok(out)
}
