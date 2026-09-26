//! `profile`: reduce a decoded row sample into per-column facts.
//!
//! The command is one function: [`profile`] takes a
//! [`crate::third_party::parquet::api::Sample`] plus a [`ProfileRequest`] and
//! returns a [`Profile`]: for each selected column, nulls, distinct values,
//! entropy, top values, lexical bounds, string-length stats, run lengths, and
//! monotonicity. Unlike `bytemass` (footer only), this decodes row values.

use std::collections::HashMap;

use glob::Pattern;
use serde::Serialize;

use crate::third_party::parquet::api::Sample;

/// Errors from the profile layer: an empty sample or a bad column glob.
#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "profile: {}", self.0)
    }
}

impl std::error::Error for Error {}

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
#[derive(Debug, Clone, Serialize)]
pub struct ValueCount {
    pub value: String,
    pub count: u64,
}

/// The facts for one column over the sampled rows.
#[derive(Debug, Clone, Serialize)]
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

/// Reduce a decoded sample into per-column facts.
///
/// # Errors
/// Returns [`Error`] when the sample has no columns or no rows, or when a
/// `columns` glob does not parse.
pub fn profile(sample: &Sample, request: &ProfileRequest) -> Result<Profile, Error> {
    if sample.columns.is_empty() {
        return Err(Error("sample has no columns".into()));
    }
    if sample.rows.is_empty() {
        return Err(Error("sample has no rows".into()));
    }
    let patterns = compile_patterns(&request.columns)?;
    let mut columns = Vec::new();
    for (index, name) in sample.columns.iter().enumerate() {
        if !patterns.is_empty() && !patterns.iter().any(|pattern| pattern.matches(name)) {
            continue;
        }
        let cells: Vec<Option<String>> = sample
            .rows
            .iter()
            .map(|row| row.get(index).cloned().flatten())
            .collect();
        columns.push(column_profile(name, &cells, request.top));
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
        .map(|pattern| Pattern::new(pattern).map_err(|error| Error(format!("bad glob: {error}"))))
        .collect()
}

/// Build one column's facts from its sampled cells.
fn column_profile(column: &str, cells: &[Option<String>], top: u32) -> ColumnProfile {
    let num_values = cells.len() as u64;
    let null_count = cells.iter().filter(|cell| cell.is_none()).count() as u64;
    let values: Vec<&str> = cells.iter().filter_map(|cell| cell.as_deref()).collect();

    let mut counts: HashMap<&str, u64> = HashMap::new();
    for value in &values {
        *counts.entry(value).or_insert(0) += 1;
    }
    let non_null = values.len();
    let ndv = counts.len() as u64;
    let entropy = shannon(&counts, non_null);
    let top_values = top_values(&counts, top);

    let min_value = values.iter().min().map(|value| (*value).to_string());
    let max_value = values.iter().max().map(|value| (*value).to_string());

    let mut lengths: Vec<u64> = values.iter().map(|value| value.len() as u64).collect();
    lengths.sort_unstable();
    let (length_mean, length_p50, length_p90, length_max) = length_stats(&lengths);

    let (adjacent_equal_fraction, run_length_mean, run_length_max) = run_stats(cells);

    ColumnProfile {
        column: column.to_string(),
        physical_kind: physical_kind(&values).to_string(),
        num_values,
        null_count,
        null_fraction: ratio(null_count, num_values),
        ndv,
        ndv_ratio: ratio(ndv, num_values),
        entropy,
        top_values,
        min_value,
        max_value,
        length_mean,
        length_p50,
        length_p90,
        length_max,
        adjacent_equal_fraction,
        run_length_mean,
        run_length_max,
        monotonic: monotonic(&values).to_string(),
    }
}

/// `numerator / denominator` as `f64`, or `0.0` when `denominator` is zero.
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// Shannon entropy in bits over the value frequencies.
fn shannon(counts: &HashMap<&str, u64>, total: usize) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let total = total as f64;
    counts
        .values()
        .map(|count| {
            let p = *count as f64 / total;
            -p * p.log2()
        })
        .sum()
}

/// The `top` values by count descending, then value ascending.
fn top_values(counts: &HashMap<&str, u64>, top: u32) -> Vec<ValueCount> {
    let mut ordered: Vec<(&str, u64)> = counts
        .iter()
        .map(|(value, count)| (*value, *count))
        .collect();
    ordered.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    ordered
        .into_iter()
        .take(top as usize)
        .map(|(value, count)| ValueCount {
            value: value.to_string(),
            count,
        })
        .collect()
}

/// The inferred kind of a column from its stringified non-null values.
fn physical_kind(values: &[&str]) -> &'static str {
    if values.is_empty() {
        return "empty";
    }
    if values
        .iter()
        .all(|value| *value == "true" || *value == "false")
    {
        return "boolean";
    }
    if values.iter().all(|value| value.parse::<i64>().is_ok()) {
        return "integer";
    }
    if values.iter().all(|value| value.parse::<f64>().is_ok()) {
        return "number";
    }
    "string"
}

/// Mean, p50, p90, and max of the (sorted) string byte lengths.
fn length_stats(sorted: &[u64]) -> (Option<f64>, Option<u64>, Option<u64>, Option<u64>) {
    if sorted.is_empty() {
        return (None, None, None, None);
    }
    let mean = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;
    (
        Some(mean),
        Some(percentile(sorted, 0.5)),
        Some(percentile(sorted, 0.9)),
        Some(*sorted.last().unwrap()),
    )
}

/// Nearest-rank percentile over a sorted, non-empty slice.
fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    let rank = (fraction * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

/// `(adjacent_equal_fraction, mean_run_length, max_run_length)` over cells.
///
/// Nulls count as a value: two adjacent nulls are equal.
fn run_stats(cells: &[Option<String>]) -> (f64, f64, u64) {
    if cells.is_empty() {
        return (0.0, 0.0, 0);
    }
    let mut runs = 0u64;
    let mut run_length = 0u64;
    let mut run_length_max = 0u64;
    let mut previous: Option<&Option<String>> = None;
    for cell in cells {
        if previous == Some(cell) {
            run_length += 1;
        } else {
            run_length = 1;
            runs += 1;
        }
        run_length_max = run_length_max.max(run_length);
        previous = Some(cell);
    }
    let adjacent_equal_fraction = if cells.len() < 2 {
        0.0
    } else {
        (cells.len() as u64 - runs) as f64 / (cells.len() as u64 - 1) as f64
    };
    (
        adjacent_equal_fraction,
        cells.len() as f64 / runs as f64,
        run_length_max,
    )
}

/// Classify the non-null values' order, ignoring nulls.
fn monotonic(values: &[&str]) -> &'static str {
    if values.len() <= 1 {
        return "CONSTANT";
    }
    let increasing = values.windows(2).all(|pair| pair[0] <= pair[1]);
    let decreasing = values.windows(2).all(|pair| pair[0] >= pair[1]);
    if increasing && decreasing {
        "CONSTANT"
    } else if increasing {
        "INCREASING"
    } else if decreasing {
        "DECREASING"
    } else {
        "UNORDERED"
    }
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
