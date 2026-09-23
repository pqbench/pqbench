//! Sample-level column facts from a dump.
//!
//! [`profile`] reads an in-memory [`Dump`] and writes one [`ColumnProfile`]
//! per column. Default work is one pass plus a sort of the sample
//! (`O(n log n)` per column). Pairwise dependency facts are off until
//! [`ProfileRequest::dependencies`] is set (`O(columns² · n)`).

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::dump::Dump;
use crate::parquet_helpers::Error;
use crate::pattern;

/// Arguments for [`profile`].
#[derive(Debug, Clone)]
pub struct ProfileRequest {
    /// Keep columns whose names match these globs. Empty keeps every column.
    pub columns: Vec<String>,
    /// How many heavy-hitter values to keep per column.
    pub top: u32,
    /// Also emit pairwise dependency facts (`O(columns² · n)`).
    pub dependencies: bool,
}

impl Default for ProfileRequest {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            top: 8,
            dependencies: false,
        }
    }
}

/// Sample-level facts: columns, optional pairs, and a capability list.
#[derive(Debug, Clone, Serialize)]
pub struct Profile {
    /// Rows that were profiled.
    pub num_rows: u64,
    /// Actions this command can take and their cost.
    pub capabilities: Vec<Capability>,
    /// One fact row per selected column.
    pub columns: Vec<ColumnProfile>,
    /// Pairwise facts, empty unless [`ProfileRequest::dependencies`].
    pub dependencies: Vec<DependencyProfile>,
}

/// One measurement an agent can request.
#[derive(Debug, Clone, Serialize)]
pub struct Capability {
    /// Action name (`column`, `dependencies`).
    pub name: String,
    /// `cheap` (default) or `medium` (opt-in).
    pub cost: String,
    /// CLI flag that enables this action, when it is not the default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flag: Option<String>,
    /// Fact names this action returns.
    pub returns: Vec<String>,
}

/// Facts for one column on the sample.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnProfile {
    /// Column name.
    pub column: String,
    /// Coarse value kind inferred from the sample.
    pub physical_kind: String,
    /// Values in the sample, including nulls.
    pub num_values: u64,
    /// Nulls in the sample.
    pub null_count: u64,
    /// `null_count / num_values`.
    pub null_fraction: f64,
    /// Distinct non-null values in the sample.
    pub ndv: u64,
    /// `ndv / (num_values - null_count)`.
    pub ndv_ratio: f64,
    /// Shannon entropy of the sample, in bits.
    pub entropy: f64,
    /// Most frequent non-null values.
    pub top_values: Vec<ValueCount>,
    /// Minimum, when values compare.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_value: Option<String>,
    /// Maximum, when values compare.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_value: Option<String>,
    /// Arithmetic mean, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean: Option<f64>,
    /// Population standard deviation, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stddev: Option<f64>,
    /// 50th percentile, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantile_p50: Option<f64>,
    /// 90th percentile, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantile_p90: Option<f64>,
    /// 99th percentile, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantile_p99: Option<f64>,
    /// Mean adjacent absolute delta, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_delta_mean: Option<f64>,
    /// String / binary length p50.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length_p50: Option<u64>,
    /// String / binary length p90.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length_p90: Option<u64>,
    /// String / binary length p99.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length_p99: Option<u64>,
    /// Longest string / binary value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length_max: Option<u64>,
    /// Shared prefix length of every non-null string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub common_prefix_length: Option<u64>,
    /// Shared suffix length of every non-null string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub common_suffix_length: Option<u64>,
    /// Mean adjacent shared-prefix length.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_prefix_mean: Option<f64>,
    /// Fraction of adjacent pairs that are equal.
    pub adjacent_equal_fraction: f64,
    /// Mean run length of equal values.
    pub run_length_mean: f64,
    /// `CONSTANT`, `INCREASING`, `DECREASING`, or `UNORDERED`.
    pub monotonic: String,
    /// Observed representations (`uuid`, `enum_like`, …).
    pub patterns: Vec<String>,
}

/// One heavy-hitter value and its count.
#[derive(Debug, Clone, Serialize)]
pub struct ValueCount {
    /// Wire form of the value.
    pub value: String,
    /// Occurrences in the sample.
    pub count: u64,
}

/// Pairwise dependency facts for two columns.
#[derive(Debug, Clone, Serialize)]
pub struct DependencyProfile {
    /// Left column name.
    pub left: String,
    /// Right column name.
    pub right: String,
    /// Distinct left values (including null as one key).
    pub ndv_left: u64,
    /// Distinct right values.
    pub ndv_right: u64,
    /// Distinct `(left, right)` pairs.
    pub ndv_pair: u64,
    /// `H(left)` in bits.
    pub entropy_left: f64,
    /// `H(right)` in bits.
    pub entropy_right: f64,
    /// `H(left, right)` in bits.
    pub entropy_pair: f64,
    /// `I(left; right)` in bits.
    pub mutual_information: f64,
    /// `2 I / (H(left) + H(right))`.
    pub normalized_mutual_information: f64,
    /// `1 - H(right|left) / H(right)` when `H(right) > 0`.
    pub functional_dependency_right: f64,
    /// `1 - H(left|right) / H(left)` when `H(left) > 0`.
    pub functional_dependency_left: f64,
}

/// Measure sample-level facts on a dump.
///
/// # Errors
/// Fails when a column glob is invalid or no column remains after filtering.
pub fn profile(dump: &Dump, request: &ProfileRequest) -> Result<Profile, Error> {
    if dump.rows.is_empty() {
        return Err(Error("sample has no rows".into()));
    }
    let selected = selected_columns(dump, request)?;
    if selected.is_empty() {
        return Err(Error("no columns matched --columns".into()));
    }
    let top = request.top.max(1) as usize;
    let columns: Vec<ColumnProfile> = selected
        .iter()
        .map(|&index| profile_column(&dump.columns[index], column_values(dump, index), top))
        .collect();
    let dependencies = if request.dependencies {
        pair_dependencies(dump, &selected)
    } else {
        Vec::new()
    };
    Ok(Profile {
        num_rows: dump.rows.len() as u64,
        capabilities: capabilities(),
        columns,
        dependencies,
    })
}

fn capabilities() -> Vec<Capability> {
    vec![
        Capability {
            name: "column".into(),
            cost: "cheap".into(),
            flag: None,
            returns: vec![
                "ndv".into(),
                "null_fraction".into(),
                "entropy".into(),
                "top_values".into(),
                "quantiles".into(),
                "patterns".into(),
            ],
        },
        Capability {
            name: "dependencies".into(),
            cost: "medium".into(),
            flag: Some("--dependencies".into()),
            returns: vec![
                "mutual_information".into(),
                "normalized_mutual_information".into(),
                "functional_dependency".into(),
            ],
        },
    ]
}

fn selected_columns(dump: &Dump, request: &ProfileRequest) -> Result<Vec<usize>, Error> {
    let mut indexes = Vec::new();
    for (index, name) in dump.columns.iter().enumerate() {
        if pattern::keep(name, &request.columns, &[]).map_err(|error| Error(error.to_string()))? {
            indexes.push(index);
        }
    }
    Ok(indexes)
}

fn column_values(dump: &Dump, index: usize) -> Vec<&Value> {
    dump.rows
        .iter()
        .map(|row| row.get(index).unwrap_or(&Value::Null))
        .collect()
}

fn profile_column(name: &str, values: Vec<&Value>, top: usize) -> ColumnProfile {
    let num_values = values.len() as u64;
    let mut null_count = 0u64;
    let mut counts = BTreeMap::new();
    let mut numbers = Vec::new();
    let mut texts = Vec::new();
    let mut adjacent_equal = 0u64;
    let mut adjacent_pairs = 0u64;
    let mut run_lengths = Vec::new();
    let mut run_length = 0u64;
    let mut previous: Option<&Value> = None;
    let mut previous_number: Option<f64> = None;
    let mut deltas = Vec::new();
    let mut prefix_lengths = Vec::new();
    let mut previous_text: Option<&str> = None;
    let mut rising = 0u64;
    let mut falling = 0u64;
    let mut compared = 0u64;

    for value in &values {
        if value.is_null() {
            null_count += 1;
            finish_run(&mut run_lengths, &mut run_length);
            previous = Some(*value);
            previous_number = None;
            previous_text = None;
            continue;
        }
        *counts.entry(value_key(value)).or_insert(0) += 1;
        if let Some(number) = as_number(value) {
            numbers.push(number);
            if let Some(last) = previous_number {
                deltas.push((number - last).abs());
                compared += 1;
                if number > last {
                    rising += 1;
                } else if number < last {
                    falling += 1;
                }
            }
            previous_number = Some(number);
        } else {
            previous_number = None;
        }
        if let Some(text) = as_text(value) {
            texts.push(text);
            if let Some(last) = previous_text {
                prefix_lengths.push(shared_prefix(last, text) as u64);
                match last.cmp(text) {
                    std::cmp::Ordering::Less => {
                        compared += 1;
                        rising += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        compared += 1;
                        falling += 1;
                    }
                    std::cmp::Ordering::Equal => {}
                }
            }
            previous_text = Some(text);
        } else {
            previous_text = None;
        }
        if let Some(last) = previous {
            if !last.is_null() {
                adjacent_pairs += 1;
                if last == *value {
                    adjacent_equal += 1;
                    run_length += 1;
                } else {
                    finish_run(&mut run_lengths, &mut run_length);
                    run_length = 1;
                }
            } else {
                run_length = 1;
            }
        } else {
            run_length = 1;
        }
        previous = Some(*value);
    }
    finish_run(&mut run_lengths, &mut run_length);

    let non_null = num_values.saturating_sub(null_count);
    let ndv = counts.len() as u64;
    let entropy = shannon(&counts.values().copied().collect::<Vec<_>>());
    let mut ranked: Vec<_> = counts.into_iter().collect();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    ranked.truncate(top);
    let top_values = ranked
        .into_iter()
        .map(|(value, count)| ValueCount { value, count })
        .collect();

    let (min_value, max_value) = bounds(&numbers, &texts);
    let (mean, stddev) = moments(&numbers);
    let quantile_p50 = quantile(&numbers, 0.50);
    let quantile_p90 = quantile(&numbers, 0.90);
    let quantile_p99 = quantile(&numbers, 0.99);
    let adjacent_delta_mean = mean_of(&deltas);
    let lengths: Vec<u64> = texts.iter().map(|text| text.len() as u64).collect();
    let length_p50 = quantile_u64(&lengths, 0.50);
    let length_p90 = quantile_u64(&lengths, 0.90);
    let length_p99 = quantile_u64(&lengths, 0.99);
    let length_max = lengths.iter().copied().max();

    ColumnProfile {
        column: name.to_string(),
        physical_kind: physical_kind(&values).to_string(),
        num_values,
        null_count,
        null_fraction: fraction(null_count, num_values),
        ndv,
        ndv_ratio: fraction(ndv, non_null),
        entropy,
        top_values,
        min_value,
        max_value,
        mean,
        stddev,
        quantile_p50,
        quantile_p90,
        quantile_p99,
        adjacent_delta_mean,
        length_p50,
        length_p90,
        length_p99,
        length_max,
        common_prefix_length: common_affix(&texts, true),
        common_suffix_length: common_affix(&texts, false),
        adjacent_prefix_mean: mean_of(
            &prefix_lengths.iter().map(|n| *n as f64).collect::<Vec<_>>(),
        ),
        adjacent_equal_fraction: fraction(adjacent_equal, adjacent_pairs),
        run_length_mean: mean_of(&run_lengths.iter().map(|n| *n as f64).collect::<Vec<_>>())
            .unwrap_or(0.0),
        monotonic: monotonic(compared, rising, falling).to_string(),
        patterns: patterns(&texts, ndv, non_null),
    }
}

fn pair_dependencies(dump: &Dump, selected: &[usize]) -> Vec<DependencyProfile> {
    let mut out = Vec::new();
    for (offset, &left) in selected.iter().enumerate() {
        for &right in selected.iter().skip(offset + 1) {
            out.push(pair_dependency(
                &dump.columns[left],
                &dump.columns[right],
                &column_values(dump, left),
                &column_values(dump, right),
            ));
        }
    }
    out
}

fn pair_dependency(
    left_name: &str,
    right_name: &str,
    left: &[&Value],
    right: &[&Value],
) -> DependencyProfile {
    let mut left_counts = BTreeMap::new();
    let mut right_counts = BTreeMap::new();
    let mut pair_counts = BTreeMap::new();
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let left_key = value_key(left_value);
        let right_key = value_key(right_value);
        *left_counts.entry(left_key.clone()).or_insert(0) += 1;
        *right_counts.entry(right_key.clone()).or_insert(0) += 1;
        *pair_counts.entry((left_key, right_key)).or_insert(0) += 1;
    }
    let entropy_left = shannon(&left_counts.values().copied().collect::<Vec<_>>());
    let entropy_right = shannon(&right_counts.values().copied().collect::<Vec<_>>());
    let entropy_pair = shannon(&pair_counts.values().copied().collect::<Vec<_>>());
    let mutual = (entropy_left + entropy_right - entropy_pair).max(0.0);
    let denom = entropy_left + entropy_right;
    DependencyProfile {
        left: left_name.to_string(),
        right: right_name.to_string(),
        ndv_left: left_counts.len() as u64,
        ndv_right: right_counts.len() as u64,
        ndv_pair: pair_counts.len() as u64,
        entropy_left,
        entropy_right,
        entropy_pair,
        mutual_information: mutual,
        normalized_mutual_information: if denom > 0.0 {
            2.0 * mutual / denom
        } else {
            0.0
        },
        functional_dependency_right: functional_dependency(
            entropy_right,
            entropy_pair - entropy_left,
        ),
        functional_dependency_left: functional_dependency(
            entropy_left,
            entropy_pair - entropy_right,
        ),
    }
}

fn functional_dependency(entropy: f64, conditional: f64) -> f64 {
    if entropy <= 0.0 {
        return 0.0;
    }
    (1.0 - (conditional.max(0.0) / entropy)).clamp(0.0, 1.0)
}

fn physical_kind(values: &[&Value]) -> &'static str {
    let mut saw_bool = false;
    let mut saw_int = false;
    let mut saw_float = false;
    let mut saw_string = false;
    let mut saw_object = false;
    for value in values {
        match value {
            Value::Null => {}
            Value::Bool(_) => saw_bool = true,
            Value::Number(number) if number.is_i64() || number.is_u64() => saw_int = true,
            Value::Number(_) => saw_float = true,
            Value::String(_) => saw_string = true,
            Value::Array(_) | Value::Object(_) => saw_object = true,
        }
    }
    if saw_object {
        "OBJECT"
    } else if saw_string {
        "STRING"
    } else if saw_float {
        "FLOAT"
    } else if saw_int {
        "INT"
    } else if saw_bool {
        "BOOLEAN"
    } else {
        "UNKNOWN"
    }
}

fn patterns(texts: &[&str], ndv: u64, non_null: u64) -> Vec<String> {
    let mut out = Vec::new();
    if texts.is_empty() {
        return out;
    }
    let sample = &texts[..texts.len().min(64)];
    if most(sample, is_uuid) {
        out.push("uuid".into());
    }
    if most(sample, is_integer_string) {
        out.push("integer_string".into());
    }
    if most(sample, is_decimal_string) {
        out.push("decimal_string".into());
    }
    if most(sample, is_timestamp_string) {
        out.push("timestamp_string".into());
    }
    if most(sample, is_ip) {
        out.push("ip".into());
    }
    if most(sample, is_url) {
        out.push("url".into());
    }
    if most(sample, is_json) {
        out.push("json".into());
    }
    if ndv > 0 && ndv <= 16 && non_null >= ndv.saturating_mul(4) {
        out.push("enum_like".into());
    }
    out
}

fn most(sample: &[&str], test: fn(&str) -> bool) -> bool {
    let hits = sample.iter().filter(|text| test(text)).count();
    hits * 5 >= sample.len() * 4
}

fn is_uuid(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() == 36
        && bytes[8] == b'-'
        && bytes[13] == b'-'
        && bytes[18] == b'-'
        && bytes[23] == b'-'
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => true,
            _ => byte.is_ascii_hexdigit(),
        })
}

fn is_integer_string(text: &str) -> bool {
    let text = text.strip_prefix('-').unwrap_or(text);
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_decimal_string(text: &str) -> bool {
    let text = text.strip_prefix('-').unwrap_or(text);
    let mut dots = 0u8;
    let mut digits = 0u8;
    for byte in text.bytes() {
        match byte {
            b'.' => dots += 1,
            b'0'..=b'9' => digits += 1,
            _ => return false,
        }
    }
    dots == 1 && digits > 0
}

fn is_timestamp_string(text: &str) -> bool {
    text.len() >= 19 && text.as_bytes().get(10) == Some(&b'T') && text.contains(':')
}

fn is_ip(text: &str) -> bool {
    let mut parts = text.split('.');
    let mut count = 0u8;
    for part in parts.by_ref() {
        count += 1;
        if count > 4 {
            return false;
        }
        if part.parse::<u8>().is_err() {
            return false;
        }
    }
    count == 4
}

fn is_url(text: &str) -> bool {
    text.starts_with("http://") || text.starts_with("https://")
}

fn is_json(text: &str) -> bool {
    let trimmed = text.trim_start();
    (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_str::<Value>(text).is_ok()
}

fn value_key(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn as_number(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_i64().map(|n| n as f64))
}

fn as_text(value: &Value) -> Option<&str> {
    value.as_str()
}

fn bounds(numbers: &[f64], texts: &[&str]) -> (Option<String>, Option<String>) {
    if let (Some(min), Some(max)) = (
        numbers.iter().copied().reduce(f64::min),
        numbers.iter().copied().reduce(f64::max),
    ) {
        return (Some(min.to_string()), Some(max.to_string()));
    }
    if let (Some(min), Some(max)) = (texts.iter().min(), texts.iter().max()) {
        return (Some((*min).to_string()), Some((*max).to_string()));
    }
    (None, None)
}

fn moments(numbers: &[f64]) -> (Option<f64>, Option<f64>) {
    if numbers.is_empty() {
        return (None, None);
    }
    let mean = numbers.iter().sum::<f64>() / numbers.len() as f64;
    let variance = numbers
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / numbers.len() as f64;
    (Some(mean), Some(variance.sqrt()))
}

fn quantile(numbers: &[f64], q: f64) -> Option<f64> {
    if numbers.is_empty() {
        return None;
    }
    let mut sorted = numbers.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let index = ((sorted.len() - 1) as f64 * q).round() as usize;
    Some(sorted[index])
}

fn quantile_u64(values: &[u64], q: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) as f64 * q).round() as usize;
    Some(sorted[index])
}

fn mean_of(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn common_affix(texts: &[&str], prefix: bool) -> Option<u64> {
    let first = texts.first()?;
    if first.is_empty() {
        return Some(0);
    }
    let mut length = first.len().min(64);
    for text in texts.iter().skip(1) {
        length = if prefix {
            shared_prefix(first, text)
        } else {
            shared_suffix(first, text)
        }
        .min(length);
        if length == 0 {
            break;
        }
    }
    Some(length as u64)
}

fn shared_prefix(left: &str, right: &str) -> usize {
    left.bytes()
        .zip(right.bytes())
        .take(64)
        .take_while(|(a, b)| a == b)
        .count()
}

fn shared_suffix(left: &str, right: &str) -> usize {
    left.bytes()
        .rev()
        .zip(right.bytes().rev())
        .take(64)
        .take_while(|(a, b)| a == b)
        .count()
}

fn shannon(counts: &[u64]) -> f64 {
    let total = counts.iter().sum::<u64>() as f64;
    if total == 0.0 {
        return 0.0;
    }
    counts
        .iter()
        .filter(|count| **count > 0)
        .map(|count| {
            let p = *count as f64 / total;
            -p * p.log2()
        })
        .sum()
}

fn fraction(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

fn finish_run(runs: &mut Vec<u64>, length: &mut u64) {
    if *length > 0 {
        runs.push(*length);
        *length = 0;
    }
}

fn monotonic(compared: u64, rising: u64, falling: u64) -> &'static str {
    if compared == 0 {
        "CONSTANT"
    } else if falling == 0 && rising > 0 {
        "INCREASING"
    } else if rising == 0 && falling > 0 {
        "DECREASING"
    } else if rising == 0 && falling == 0 {
        "CONSTANT"
    } else {
        "UNORDERED"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dump(columns: &[&str], rows: Vec<Vec<Value>>) -> Dump {
        Dump {
            columns: columns.iter().map(|name| (*name).to_string()).collect(),
            rows,
        }
    }

    #[test]
    fn profiles_ndv_and_nulls_on_a_tiny_sample() {
        let dump = dump(
            &["id", "flag"],
            vec![
                vec![json!(1), json!(true)],
                vec![json!(1), Value::Null],
                vec![json!(2), json!(false)],
            ],
        );
        let profile = profile(&dump, &ProfileRequest::default()).unwrap();
        assert_eq!(profile.num_rows, 3);
        let id = profile
            .columns
            .iter()
            .find(|column| column.column == "id")
            .unwrap();
        assert_eq!(id.ndv, 2);
        assert_eq!(id.physical_kind, "INT");
        assert!(id.mean.is_some());
        assert!(profile.dependencies.is_empty());
        assert!(profile
            .capabilities
            .iter()
            .any(|capability| capability.name == "dependencies" && capability.flag.is_some()));
    }

    #[test]
    fn dependencies_are_opt_in() {
        let dump = dump(
            &["a", "b"],
            vec![
                vec![json!("x"), json!(1)],
                vec![json!("x"), json!(1)],
                vec![json!("y"), json!(2)],
            ],
        );
        let request = ProfileRequest {
            dependencies: true,
            ..ProfileRequest::default()
        };
        let profile = profile(&dump, &request).unwrap();
        assert_eq!(profile.dependencies.len(), 1);
        assert!(profile.dependencies[0].functional_dependency_right > 0.9);
    }

    #[test]
    fn detects_uuid_and_enum_like_strings() {
        let dump = dump(
            &["id", "country"],
            vec![
                vec![json!("550e8400-e29b-41d4-a716-446655440000"), json!("US")],
                vec![json!("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), json!("US")],
                vec![json!("6ba7b811-9dad-11d1-80b4-00c04fd430c8"), json!("DE")],
                vec![json!("6ba7b812-9dad-11d1-80b4-00c04fd430c8"), json!("US")],
                vec![json!("6ba7b813-9dad-11d1-80b4-00c04fd430c8"), json!("DE")],
                vec![json!("6ba7b814-9dad-11d1-80b4-00c04fd430c8"), json!("US")],
                vec![json!("6ba7b815-9dad-11d1-80b4-00c04fd430c8"), json!("US")],
                vec![json!("6ba7b816-9dad-11d1-80b4-00c04fd430c8"), json!("DE")],
            ],
        );
        let profile = profile(&dump, &ProfileRequest::default()).unwrap();
        let id = profile
            .columns
            .iter()
            .find(|column| column.column == "id")
            .unwrap();
        assert!(id.patterns.iter().any(|pattern| pattern == "uuid"));
        let country = profile
            .columns
            .iter()
            .find(|column| column.column == "country")
            .unwrap();
        assert!(country
            .patterns
            .iter()
            .any(|pattern| pattern == "enum_like"));
    }
}
