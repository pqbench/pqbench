//! `profile`: reduce a decoded row sample into per-column facts.
//!
//! The command is one function: [`profile`] takes a
//! [`crate::third_party::parquet::api::Sample`] plus a [`ProfileRequest`] and
//! returns a [`Profile`]: for each selected column, nulls, distinct values,
//! entropy, top values, lexical bounds, string-length stats, run lengths, and
//! monotonicity. Unlike `bytemass` (footer only), this decodes row values.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::Path;

use glob::Pattern;
use parquet::basic::Type as PhysicalType;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::DataType;
use parquet::file::reader::{FileReader, RowGroupReader, SerializedFileReader};
use serde::Serialize;

use crate::third_party::parquet::api::{Error as ParquetError, Sample};

/// Errors from the profile layer: an empty sample, bad column glob, or parquet
/// read failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The sample had no rows.
    Empty,
    /// No column matched the requested globs.
    NoMatch(String),
    /// A column glob did not parse.
    Pattern(glob::PatternError),
    /// Reading the Parquet file failed.
    Parquet(ParquetError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Empty => write!(f, "sample has no rows"),
            Error::NoMatch(columns) => write!(f, "no column matched: {columns}"),
            Error::Pattern(_) => write!(f, "bad glob"),
            Error::Parquet(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Pattern(error) => Some(error),
            Error::Parquet(error) => Some(error),
            Error::Empty | Error::NoMatch(_) => None,
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

/// Which columns to profile and how many top values to keep.
#[derive(Debug, Clone)]
pub struct ProfileRequest {
    /// `glob::Pattern` globs matched against column names; empty keeps all.
    pub columns: Vec<String>,
    /// Number of top values reported per column.
    pub top: u32,
}

impl Default for ProfileRequest {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            top: 8,
        }
    }
}

/// One frequent value and how many times it occurred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ValueCount {
    pub value: String,
    pub count: u64,
}

/// The facts for one column over the sampled rows.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ColumnProfile {
    pub column: String,
    /// One of `empty`, `boolean`, `integer`, `number`, `string`.
    pub physical_kind: String,
    // aipnaming: allow(aip-141/count-suffix)
    pub num_values: u64,
    // aipnaming: allow(aip-141/count-suffix)
    pub null_count: u64,
    pub null_fraction: f64,
    pub ndv: u64,
    pub ndv_ratio: f64,
    /// Shannon entropy in bits over non-null values.
    pub entropy: f64,
    pub top_values: Vec<ValueCount>,
    /// Lexical minimum over non-null values.
    // aipnaming: allow(aip-145/ranges)
    pub min_value: Option<String>,
    /// Lexical maximum over non-null values.
    // aipnaming: allow(aip-145/ranges)
    pub max_value: Option<String>,
    pub length_mean: Option<f64>,
    pub length_p50: Option<u64>,
    pub length_p90: Option<u64>,
    pub length_max: Option<u64>,
    pub adjacent_equal_fraction: f64,
    pub run_length_mean: f64,
    pub run_length_max: u64,
    /// One of `CONSTANT`, `INCREASING`, `DECREASING`, `UNORDERED`.
    pub monotonic: String,
}

/// A whole sample's profile: the row count and one entry per selected column.
#[derive(Debug, Clone, Serialize)]
pub struct Profile {
    pub row_count: u64,
    pub columns: Vec<ColumnProfile>,
}

/// Compute a [`Profile`] directly from a local Parquet file, decoding only
/// columns selected by `request.columns` up to `max_rows` leading rows.
///
/// Uses each column's Parquet physical type rather than inferring types from
/// stringified values, and supports nested leaf paths (for example `a.b`).
///
/// # Errors
/// Returns [`Error::Empty`] if the file has no rows, [`Error::NoMatch`] if
/// `request.columns` matched nothing, [`Error::Pattern`] on an invalid glob, or
/// [`Error::Parquet`] if the file cannot be read.
pub fn profile_file(
    path: &Path,
    max_rows: Option<usize>,
    request: &ProfileRequest,
) -> Result<Profile, Error> {
    let patterns = compile_patterns(&request.columns)?;
    let file = std::fs::File::open(path).map_err(ParquetError::Io)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let total_rows: usize = metadata
        .row_groups()
        .iter()
        .map(|group| usize::try_from(group.num_rows()).unwrap_or(0))
        .sum();
    if total_rows == 0 || max_rows == Some(0) {
        return Err(Error::Empty);
    }

    let schema = metadata.file_metadata().schema_descr();
    let selected: Vec<(usize, String, PhysicalType, i16, i16)> = schema
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, descr)| {
            let name = descr.path().string();
            let matches = patterns.is_empty() || patterns.iter().any(|p| p.matches(&name));
            matches.then_some((
                idx,
                name,
                descr.physical_type(),
                descr.max_def_level(),
                descr.max_rep_level(),
            ))
        })
        .collect();
    if !patterns.is_empty() && selected.is_empty() {
        return Err(Error::NoMatch(request.columns.join(", ")));
    }

    let mut accumulators: Vec<ColumnAcc> = selected
        .iter()
        .map(|(_, name, physical, _, _)| ColumnAcc::new(name.clone(), physical_kind(*physical)))
        .collect();

    let mut rows_read = 0usize;
    for rg in 0..metadata.num_row_groups() {
        let group = reader.get_row_group(rg)?;
        let group_rows = usize::try_from(group.metadata().num_rows()).unwrap_or(0);
        let take = match max_rows {
            Some(limit) => limit.saturating_sub(rows_read).min(group_rows),
            None => group_rows,
        };
        if take == 0 {
            break;
        }
        for (slot, &(col_idx, _, _, max_def, max_rep)) in selected.iter().enumerate() {
            stream_column(
                group.as_ref(),
                col_idx,
                max_def,
                max_rep,
                take,
                &mut accumulators[slot],
            )?;
        }
        rows_read += take;
        if max_rows.is_some_and(|limit| rows_read >= limit) {
            break;
        }
    }

    let columns = accumulators
        .into_iter()
        .map(|acc| acc.finish(request.top))
        .collect();
    Ok(Profile {
        row_count: rows_read as u64,
        columns,
    })
}

/// Reduce a decoded sample into per-column facts.
///
/// # Errors
/// Returns [`Error`] when the sample has no columns or no rows, or when a
/// `columns` glob does not parse.
pub fn profile(sample: &Sample, request: &ProfileRequest) -> Result<Profile, Error> {
    if sample.rows.is_empty() {
        return Err(Error::Empty);
    }
    let patterns = compile_patterns(&request.columns)?;
    let mut columns = Vec::new();
    for (index, name) in sample.columns.iter().enumerate() {
        if !patterns.is_empty() && !patterns.iter().any(|pattern| pattern.matches(name)) {
            continue;
        }
        let non_null: Vec<&str> = sample
            .rows
            .iter()
            .filter_map(|row| row.get(index).and_then(|cell| cell.as_deref()))
            .collect();
        let kind = infer_kind(&non_null);
        let mut acc = ColumnAcc::new(name.clone(), kind);
        for row in &sample.rows {
            match row.get(index).and_then(|cell| cell.as_deref()) {
                Some(text) => acc.push_parsed(text),
                None => acc.push_null(),
            }
        }
        columns.push(acc.finish(request.top));
    }
    if !patterns.is_empty() && columns.is_empty() {
        return Err(Error::NoMatch(request.columns.join(", ")));
    }
    Ok(Profile {
        row_count: sample.rows.len() as u64,
        columns,
    })
}

/// Parse the request globs, preserving order and rejecting bad patterns.
fn compile_patterns(patterns: &[String]) -> Result<Vec<Pattern>, Error> {
    patterns
        .iter()
        .map(|pattern| Pattern::new(pattern).map_err(Error::Pattern))
        .collect()
}

/// `numerator / denominator` as `f64`, or `0.0` when `denominator` is zero.
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// The profile kind for a Parquet physical type.
fn physical_kind(physical: PhysicalType) -> &'static str {
    match physical {
        PhysicalType::BOOLEAN => "boolean",
        PhysicalType::INT32 | PhysicalType::INT64 => "integer",
        PhysicalType::FLOAT | PhysicalType::DOUBLE => "number",
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY | PhysicalType::INT96 => {
            "string"
        }
    }
}

/// Infer a kind from stringified values (used only by [`profile`] on untyped
/// [`Sample`] inputs).
fn infer_kind(values: &[&str]) -> &'static str {
    if values.is_empty() {
        "empty"
    } else if values.iter().all(|v| *v == "true" || *v == "false") {
        "boolean"
    } else if values.iter().all(|v| v.parse::<i64>().is_ok()) {
        "integer"
    } else if values.iter().all(|v| v.parse::<f64>().is_ok()) {
        "number"
    } else {
        "string"
    }
}

/// Mean, p50, p90, and max of the (sorted) string byte lengths.
fn length_stats(sorted: &[u64]) -> (Option<f64>, Option<u64>, Option<u64>, Option<u64>) {
    if sorted.is_empty() {
        return (None, None, None, None);
    }
    let mean = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;
    let percentile = |fraction: f64| {
        let rank = (fraction * sorted.len() as f64).ceil() as usize;
        sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
    };
    (
        Some(mean),
        Some(percentile(0.5)),
        Some(percentile(0.9)),
        Some(*sorted.last().unwrap()),
    )
}

/// Typed sort key for bounds and monotonicity checks: exact `i64` for integers,
/// `f64::total_cmp` for floats, lexical for text/booleans.
#[derive(Clone)]
enum OrderKey {
    Integer(i64),
    Number(f64),
    Text(String),
}

impl OrderKey {
    fn cmp_key(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Number(a), Self::Number(b)) => a.total_cmp(b),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }

    fn cmp_str(&self, other: &str) -> Ordering {
        match self {
            Self::Text(a) => a.as_str().cmp(other),
            _ => Ordering::Equal,
        }
    }

    fn into_string(self) -> String {
        match self {
            Self::Integer(v) => v.to_string(),
            Self::Number(v) => v.to_string(),
            Self::Text(v) => v,
        }
    }
}

/// Streaming per-column profile accumulator shared by [`profile`] and
/// [`profile_file`].
struct ColumnAcc {
    column: String,
    kind: &'static str,
    value_count: u64,
    null_count: u64,
    counts: HashMap<String, u64>,
    lengths: Vec<u64>,
    lowest: Option<OrderKey>,
    highest: Option<OrderKey>,
    prev_cell: Option<Option<String>>,
    runs: u64,
    run_length: u64,
    run_length_max: u64,
    prev_key: Option<OrderKey>,
    increasing: bool,
    decreasing: bool,
}

impl ColumnAcc {
    fn new(column: String, kind: &'static str) -> Self {
        Self {
            column,
            kind,
            value_count: 0,
            null_count: 0,
            counts: HashMap::new(),
            lengths: Vec::new(),
            lowest: None,
            highest: None,
            prev_cell: None,
            runs: 0,
            run_length: 0,
            run_length_max: 0,
            prev_key: None,
            increasing: true,
            decreasing: true,
        }
    }

    fn push_null(&mut self) {
        self.value_count += 1;
        self.null_count += 1;
        self.push_run(None);
    }

    fn push_parsed(&mut self, text: &str) {
        match self.kind {
            "integer" => {
                if let Ok(value) = text.parse::<i64>() {
                    self.push_key(text, OrderKey::Integer(value));
                } else {
                    self.push_text(text);
                }
            }
            "number" => {
                if let Ok(value) = text.parse::<f64>() {
                    self.push_key(text, OrderKey::Number(value));
                } else {
                    self.push_text(text);
                }
            }
            _ => self.push_text(text),
        }
    }

    fn push_text(&mut self, text: &str) {
        self.value_count += 1;
        self.lengths.push(text.len() as u64);
        if self
            .lowest
            .as_ref()
            .is_none_or(|cur| cur.cmp_str(text) == Ordering::Greater)
        {
            self.lowest = Some(OrderKey::Text(text.to_string()));
        }
        if self
            .highest
            .as_ref()
            .is_none_or(|cur| cur.cmp_str(text) == Ordering::Less)
        {
            self.highest = Some(OrderKey::Text(text.to_string()));
        }
        if let Some(prev) = &self.prev_key {
            match prev.cmp_str(text) {
                Ordering::Greater => self.increasing = false,
                Ordering::Less => self.decreasing = false,
                Ordering::Equal => {}
            }
        }
        if self
            .prev_key
            .as_ref()
            .is_none_or(|prev| prev.cmp_str(text) != Ordering::Equal)
        {
            self.prev_key = Some(OrderKey::Text(text.to_string()));
        }
        self.record_cell(text);
    }

    fn push_key(&mut self, text: &str, key: OrderKey) {
        self.value_count += 1;
        self.lengths.push(text.len() as u64);
        if self
            .lowest
            .as_ref()
            .is_none_or(|cur| key.cmp_key(cur) == Ordering::Less)
        {
            self.lowest = Some(key.clone());
        }
        if self
            .highest
            .as_ref()
            .is_none_or(|cur| key.cmp_key(cur) == Ordering::Greater)
        {
            self.highest = Some(key.clone());
        }
        if let Some(prev) = &self.prev_key {
            match prev.cmp_key(&key) {
                Ordering::Greater => self.increasing = false,
                Ordering::Less => self.decreasing = false,
                Ordering::Equal => {}
            }
        }
        self.prev_key = Some(key);
        self.record_cell(text);
    }

    fn record_cell(&mut self, text: &str) {
        self.push_run(Some(text));
        if let Some(count) = self.counts.get_mut(text) {
            *count += 1;
        } else {
            self.counts.insert(text.to_string(), 1);
        }
    }

    fn push_run(&mut self, cell: Option<&str>) {
        if self.prev_cell.as_ref().map(|prev| prev.as_deref()) == Some(cell) {
            self.run_length += 1;
        } else {
            self.run_length = 1;
            self.runs += 1;
            self.prev_cell = Some(cell.map(str::to_owned));
        }
        self.run_length_max = self.run_length_max.max(self.run_length);
    }

    fn finish(mut self, top: u32) -> ColumnProfile {
        let non_null = self.value_count - self.null_count;
        let ndv = self.counts.len() as u64;
        let entropy = if non_null == 0 {
            0.0
        } else {
            let total = non_null as f64;
            let h: f64 = self
                .counts
                .values()
                .map(|count| {
                    let p = *count as f64 / total;
                    -p * p.log2()
                })
                .sum();
            if h == 0.0 {
                0.0
            } else {
                h
            }
        };
        let top_values = if top == 0 {
            Vec::new()
        } else {
            let mut ordered: Vec<(String, u64)> = self.counts.into_iter().collect();
            ordered.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            ordered
                .into_iter()
                .take(top as usize)
                .map(|(value, count)| ValueCount { value, count })
                .collect()
        };
        self.lengths.sort_unstable();
        let (length_mean, length_p50, length_p90, length_max) = length_stats(&self.lengths);
        let (adjacent_equal_fraction, run_length_mean, run_length_max) = if self.value_count == 0 {
            (0.0, 0.0, 0)
        } else {
            let adjacent = if self.value_count < 2 {
                0.0
            } else {
                (self.value_count - self.runs) as f64 / (self.value_count - 1) as f64
            };
            (
                adjacent,
                self.value_count as f64 / self.runs as f64,
                self.run_length_max,
            )
        };
        let monotonic = if non_null <= 1 || (self.increasing && self.decreasing) {
            "CONSTANT"
        } else if self.increasing {
            "INCREASING"
        } else if self.decreasing {
            "DECREASING"
        } else {
            "UNORDERED"
        };
        let physical_kind = if non_null == 0 { "empty" } else { self.kind };
        ColumnProfile {
            column: self.column,
            physical_kind: physical_kind.to_string(),
            num_values: self.value_count,
            null_count: self.null_count,
            null_fraction: ratio(self.null_count, self.value_count),
            ndv,
            ndv_ratio: ratio(ndv, self.value_count),
            entropy,
            top_values,
            min_value: self.lowest.map(OrderKey::into_string),
            max_value: self.highest.map(OrderKey::into_string),
            length_mean,
            length_p50,
            length_p90,
            length_max,
            adjacent_equal_fraction,
            run_length_mean,
            run_length_max,
            monotonic: monotonic.to_string(),
        }
    }
}

fn stream_column(
    group: &dyn RowGroupReader,
    col_idx: usize,
    max_def: i16,
    max_rep: i16,
    rows: usize,
    acc: &mut ColumnAcc,
) -> Result<(), Error> {
    match group.get_column_reader(col_idx)? {
        ColumnReader::BoolColumnReader(mut reader) => {
            feed_typed(&mut reader, max_def, max_rep, rows, acc, |acc, &v| {
                acc.push_text(if v { "true" } else { "false" });
            })
        }
        ColumnReader::Int32ColumnReader(mut reader) => {
            feed_typed(&mut reader, max_def, max_rep, rows, acc, |acc, &v| {
                acc.push_key(&v.to_string(), OrderKey::Integer(i64::from(v)));
            })
        }
        ColumnReader::Int64ColumnReader(mut reader) => {
            feed_typed(&mut reader, max_def, max_rep, rows, acc, |acc, &v| {
                acc.push_key(&v.to_string(), OrderKey::Integer(v));
            })
        }
        ColumnReader::Int96ColumnReader(mut reader) => {
            feed_typed(&mut reader, max_def, max_rep, rows, acc, |acc, v| {
                acc.push_text(&v.to_string());
            })
        }
        ColumnReader::FloatColumnReader(mut reader) => {
            feed_typed(&mut reader, max_def, max_rep, rows, acc, |acc, &v| {
                acc.push_key(&v.to_string(), OrderKey::Number(f64::from(v)));
            })
        }
        ColumnReader::DoubleColumnReader(mut reader) => {
            feed_typed(&mut reader, max_def, max_rep, rows, acc, |acc, &v| {
                acc.push_key(&v.to_string(), OrderKey::Number(v));
            })
        }
        ColumnReader::ByteArrayColumnReader(mut reader) => {
            feed_typed(&mut reader, max_def, max_rep, rows, acc, |acc, v| {
                acc.push_text(&String::from_utf8_lossy(v.data()));
            })
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut reader) => {
            feed_typed(&mut reader, max_def, max_rep, rows, acc, |acc, v| {
                acc.push_text(&String::from_utf8_lossy(v.data()));
            })
        }
    }
}

fn feed_typed<T: DataType>(
    reader: &mut ColumnReaderImpl<T>,
    max_def: i16,
    max_rep: i16,
    rows: usize,
    acc: &mut ColumnAcc,
    mut push: impl FnMut(&mut ColumnAcc, &T::T),
) -> Result<(), Error> {
    let mut remaining = rows;
    let mut values = Vec::new();
    let mut def_levels = Vec::new();
    let mut rep_levels = Vec::new();
    while remaining > 0 {
        let batch = remaining.min(1024);
        values.clear();
        def_levels.clear();
        rep_levels.clear();
        let (records, _, levels) = reader.read_records(
            batch,
            (max_def > 0).then_some(&mut def_levels),
            (max_rep > 0).then_some(&mut rep_levels),
            &mut values,
        )?;
        let consumed = if max_rep > 0 {
            records
        } else if max_def > 0 {
            levels
        } else {
            values.len()
        };
        if consumed == 0 {
            break;
        }
        if max_def == 0 {
            for value in &values {
                push(acc, value);
            }
        } else {
            let mut at = 0;
            for &def in &def_levels[..levels] {
                if def == max_def {
                    push(acc, &values[at]);
                    at += 1;
                } else {
                    acc.push_null();
                }
            }
        }
        remaining = remaining.saturating_sub(consumed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Sample {
        let rows = [
            ["a", "7", "1", "3", "x", "a"],
            ["a", "", "2", "2", "x", "c"],
            ["b", "9", "3", "1", "x", "b"],
        ];
        Sample {
            columns: ["vals", "nulls", "inc", "dec", "const", "mixed"]
                .iter()
                .map(|name| name.to_string())
                .collect(),
            rows: rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| {
                            if cell.is_empty() {
                                None
                            } else {
                                Some(cell.to_string())
                            }
                        })
                        .collect()
                })
                .collect(),
        }
    }

    fn column<'a>(profile: &'a Profile, name: &str) -> &'a ColumnProfile {
        profile
            .columns
            .iter()
            .find(|column| column.column == name)
            .unwrap()
    }

    #[test]
    fn ndv_entropy_and_top_values() {
        let request = ProfileRequest {
            top: 2,
            ..Default::default()
        };
        let profile = profile(&sample(), &request).unwrap();
        assert_eq!(profile.row_count, 3);
        let vals = column(&profile, "vals");
        assert_eq!(vals.num_values, 3);
        assert_eq!(vals.null_count, 0);
        assert_eq!(vals.ndv, 2);
        assert!((vals.ndv_ratio - 2.0 / 3.0).abs() < 1e-12);
        assert!((vals.entropy - 0.918_295_834_054_489_6).abs() < 1e-9);
        assert_eq!(vals.min_value.as_deref(), Some("a"));
        assert_eq!(vals.max_value.as_deref(), Some("b"));
        assert_eq!(vals.top_values.len(), 2);
        assert_eq!(vals.top_values[0].value, "a");
        assert_eq!(vals.top_values[0].count, 2);
        assert_eq!(vals.top_values[1].value, "b");
        assert_eq!(vals.length_mean, Some(1.0));
        assert_eq!(vals.length_p50, Some(1));
        assert_eq!(vals.length_p90, Some(1));
        assert_eq!(vals.length_max, Some(1));
        assert_eq!(vals.physical_kind, "string");
    }

    #[test]
    fn nulls_are_counted_and_excluded_from_values() {
        let profile = profile(&sample(), &ProfileRequest::default()).unwrap();
        let nulls = column(&profile, "nulls");
        assert_eq!(nulls.num_values, 3);
        assert_eq!(nulls.null_count, 1);
        assert!((nulls.null_fraction - 1.0 / 3.0).abs() < 1e-12);
        assert_eq!(nulls.ndv, 2);
        assert_eq!(nulls.min_value.as_deref(), Some("7"));
        assert_eq!(nulls.max_value.as_deref(), Some("9"));
        assert_eq!(nulls.physical_kind, "integer");
    }

    #[test]
    fn monotonic_classifies_order() {
        let profile = profile(&sample(), &ProfileRequest::default()).unwrap();
        assert_eq!(column(&profile, "inc").monotonic, "INCREASING");
        assert_eq!(column(&profile, "dec").monotonic, "DECREASING");
        assert_eq!(column(&profile, "const").monotonic, "CONSTANT");
        assert_eq!(column(&profile, "mixed").monotonic, "UNORDERED");
    }

    #[test]
    fn run_lengths_and_adjacent_equality() {
        let profile = profile(&sample(), &ProfileRequest::default()).unwrap();
        let constant = column(&profile, "const");
        assert_eq!(constant.run_length_max, 3);
        assert_eq!(constant.run_length_mean, 3.0);
        assert_eq!(constant.adjacent_equal_fraction, 1.0);

        let mixed = column(&profile, "mixed");
        assert_eq!(mixed.run_length_max, 1);
        assert_eq!(mixed.adjacent_equal_fraction, 0.0);

        let nulls = column(&profile, "nulls");
        assert_eq!(nulls.adjacent_equal_fraction, 0.0);
    }

    #[test]
    fn columns_globs_filter_in_sample_order() {
        let request = ProfileRequest {
            columns: vec!["in*".to_string(), "dec".to_string()],
            ..Default::default()
        };
        let profiled = profile(&sample(), &request).unwrap();
        let names: Vec<&str> = profiled
            .columns
            .iter()
            .map(|column| column.column.as_str())
            .collect();
        assert_eq!(names, ["inc", "dec"]);

        let request = ProfileRequest {
            columns: vec!["*".to_string()],
            ..Default::default()
        };
        assert_eq!(profile(&sample(), &request).unwrap().columns.len(), 6);
    }

    #[test]
    fn zero_entropy_is_positive_zero() {
        let profile = profile(&sample(), &ProfileRequest::default()).unwrap();
        let constant = column(&profile, "const");
        assert_eq!(constant.entropy, 0.0);
        assert!(constant.entropy.is_sign_positive(), "{}", constant.entropy);
    }

    #[test]
    fn numeric_columns_use_numeric_order() {
        let sample = Sample {
            columns: vec!["i".to_string()],
            rows: vec![
                vec![Some("1".to_string())],
                vec![Some("2".to_string())],
                vec![Some("10".to_string())],
            ],
        };
        let profile = profile(&sample, &ProfileRequest::default()).unwrap();
        let i = column(&profile, "i");
        assert_eq!(i.physical_kind, "integer");
        assert_eq!(i.min_value.as_deref(), Some("1"));
        assert_eq!(i.max_value.as_deref(), Some("10"));
        assert_eq!(i.monotonic, "INCREASING");
    }

    #[test]
    fn unmatched_columns_error() {
        let request = ProfileRequest {
            columns: vec!["nope".to_string()],
            ..Default::default()
        };
        assert!(profile(&sample(), &request).is_err());
    }

    #[test]
    fn rejects_bad_globs() {
        let request = ProfileRequest {
            columns: vec!["[".to_string()],
            ..Default::default()
        };
        assert!(profile(&sample(), &request).is_err());
    }

    #[test]
    fn empty_sample_errors() {
        assert!(profile(&Sample::default(), &ProfileRequest::default()).is_err());
        let no_rows = Sample {
            columns: vec!["a".to_string()],
            rows: Vec::new(),
        };
        assert!(profile(&no_rows, &ProfileRequest::default()).is_err());
    }
}
