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
    /// Also emit pairwise locality facts (`O(pairs · n)`).
    pub dependencies: bool,
    /// Named pairs `(left, right)`. Empty lets L0/L2 pick promising columns.
    pub pairs: Vec<(String, String)>,
    /// Locality measures to compute. Empty with [`Self::dependencies`] is all.
    pub measures: Vec<String>,
    /// L0 compressed bytes per column path (from a footer / bytemass).
    pub masses: BTreeMap<String, u64>,
}

impl Default for ProfileRequest {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            top: 8,
            dependencies: false,
            pairs: Vec::new(),
            measures: Vec::new(),
            masses: BTreeMap::new(),
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
    /// How pairwise columns were chosen, when locality analysis ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locality: Option<Locality>,
}

/// Pairwise locality run: what was requested and which columns were paired.
#[derive(Debug, Clone, Serialize)]
pub struct Locality {
    /// `requested` (`--pairs` / narrow `--columns`) or `promising` (L0+L2).
    pub selection: String,
    /// Columns that entered the pairwise pass.
    pub columns: Vec<String>,
    /// Measure names that were computed.
    pub measures: Vec<String>,
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
    /// Full value counts when NDV is small (`<= 32`).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub frequency: Vec<ValueCount>,
    /// Values that appear once.
    pub singleton_count: u64,
    /// `singleton_count / ndv`.
    pub singleton_fraction: f64,
    /// Minimum, when values compare.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_value: Option<String>,
    /// Maximum, when values compare.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_value: Option<String>,
    /// `max - min`, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<f64>,
    /// Arithmetic mean, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean: Option<f64>,
    /// Population standard deviation, when numeric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stddev: Option<f64>,
    /// Third standardized moment, when numeric and `n >= 3`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skew: Option<f64>,
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
    /// Adjacent absolute-delta p50.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_delta_p50: Option<f64>,
    /// Adjacent absolute-delta p90.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_delta_p90: Option<f64>,
    /// Adjacent absolute-delta p99.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_delta_p99: Option<f64>,
    /// Largest adjacent absolute delta.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_delta_max: Option<f64>,
    /// Fraction of adjacent numeric deltas that are zero.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_delta_zero_fraction: Option<f64>,
    /// Mean string / binary length.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length_mean: Option<f64>,
    /// Shortest string / binary value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length_min: Option<u64>,
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
    /// Distinct code points seen in the string sample (capped scan).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_code_points: Option<u64>,
    /// Fraction of bytes that are ASCII.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ascii_fraction: Option<f64>,
    /// Fraction of bytes that are `0-9`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digit_fraction: Option<f64>,
    /// Fraction of bytes that are `A-Za-z`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub letter_fraction: Option<f64>,
    /// Fraction of bytes that are `0-9A-Fa-f`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hex_fraction: Option<f64>,
    /// Fraction of bytes that are whitespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub whitespace_fraction: Option<f64>,
    /// Fraction of adjacent pairs that are equal.
    pub adjacent_equal_fraction: f64,
    /// Mean run length of equal values.
    pub run_length_mean: f64,
    /// Run-length p50.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_length_p50: Option<u64>,
    /// Run-length p90.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_length_p90: Option<u64>,
    /// Run-length p99.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_length_p99: Option<u64>,
    /// Longest equal run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_length_max: Option<u64>,
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

/// Pairwise locality facts for two columns.
#[derive(Debug, Clone, Serialize)]
pub struct DependencyProfile {
    /// Left column name.
    pub left: String,
    /// Right column name.
    pub right: String,
    /// Distinct left values (including null as one key).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ndv_left: Option<u64>,
    /// Distinct right values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ndv_right: Option<u64>,
    /// Distinct `(left, right)` pairs. NDV(A,B).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ndv_pair: Option<u64>,
    /// Mean NDV(right | left).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ndv_right_given_left_mean: Option<f64>,
    /// Max NDV(right | left).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ndv_right_given_left_max: Option<u64>,
    /// Mean NDV(left | right).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ndv_left_given_right_mean: Option<f64>,
    /// Max NDV(left | right).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ndv_left_given_right_max: Option<u64>,
    /// `H(left)` in bits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entropy_left: Option<f64>,
    /// `H(right)` in bits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entropy_right: Option<f64>,
    /// `H(left, right)` in bits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entropy_pair: Option<f64>,
    /// `H(right|left)` in bits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entropy_right_given_left: Option<f64>,
    /// `H(left|right)` in bits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entropy_left_given_right: Option<f64>,
    /// `I(left; right)` in bits.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutual_information: Option<f64>,
    /// `2 I / (H(left) + H(right))`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalized_mutual_information: Option<f64>,
    /// `1 - H(right|left) / H(right)` when `H(right) > 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub functional_dependency_right: Option<f64>,
    /// `1 - H(left|right) / H(left)` when `H(left) > 0`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub functional_dependency_left: Option<f64>,
    /// Rows where both values are null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_both: Option<u64>,
    /// Rows where only the left value is null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_left_only: Option<u64>,
    /// Rows where only the right value is null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_right_only: Option<u64>,
    /// Rows where neither value is null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_neither: Option<u64>,
    /// `null_both / (null_both + null_left_only + null_right_only)`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_jaccard: Option<f64>,
    /// Pearson on paired non-null numbers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pearson: Option<f64>,
    /// Spearman rank correlation on paired non-null numbers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spearman: Option<f64>,
    /// Adjacent numeric steps that move in the same direction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub same_sign_delta_fraction: Option<f64>,
    /// Cramér's V on the sample contingency table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cramers_v: Option<f64>,
}

/// Measure sample-level facts on a dump.
///
/// # Errors
/// Fails when a column glob is invalid or no column remains after filtering.
pub fn profile(dump: &Dump, request: &ProfileRequest) -> Result<Profile, Error> {
    if dump.rows.is_empty() {
        return Err(Error("sample has no rows".into()));
    }
    let dump = explode_nested(dump);
    let selected = selected_columns(&dump, request)?;
    if selected.is_empty() {
        return Err(Error("no columns matched --columns".into()));
    }
    let top = request.top.max(1) as usize;
    let columns: Vec<ColumnProfile> = selected
        .iter()
        .map(|&index| profile_column(&dump.columns[index], column_values(&dump, index), top))
        .collect();
    let want = request.dependencies || !request.pairs.is_empty() || !request.measures.is_empty();
    let (dependencies, locality) = if want {
        let measures = MeasureSet::parse(&request.measures)?;
        let (pairs, locality) = locality_pairs(&dump, &columns, &selected, request)?;
        let dependencies = pairs
            .iter()
            .map(|&(left, right)| {
                pair_dependency(
                    &dump.columns[left],
                    &dump.columns[right],
                    &column_values(&dump, left),
                    &column_values(&dump, right),
                    &measures,
                )
            })
            .collect();
        (dependencies, Some(locality))
    } else {
        (Vec::new(), None)
    };
    Ok(Profile {
        num_rows: dump.rows.len() as u64,
        capabilities: capabilities(),
        columns,
        dependencies,
        locality,
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
                "frequency".into(),
                "range".into(),
                "quantiles".into(),
                "skew".into(),
                "adjacent_delta".into(),
                "run_lengths".into(),
                "alphabet".into(),
                "patterns".into(),
            ],
        },
        Capability {
            name: "locality".into(),
            cost: "medium".into(),
            flag: Some("--dependencies".into()),
            returns: MEASURE_NAMES
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
        },
        Capability {
            name: "pair_ndv".into(),
            cost: "medium".into(),
            flag: Some("--measures pair_ndv".into()),
            returns: vec![
                "ndv_pair".into(),
                "ndv_right_given_left".into(),
                "ndv_left_given_right".into(),
            ],
        },
        Capability {
            name: "entropy".into(),
            cost: "medium".into(),
            flag: Some("--measures entropy".into()),
            returns: vec![
                "entropy_left".into(),
                "entropy_right".into(),
                "entropy_right_given_left".into(),
                "entropy_left_given_right".into(),
            ],
        },
        Capability {
            name: "mutual_information".into(),
            cost: "medium".into(),
            flag: Some("--measures mutual_information".into()),
            returns: vec![
                "mutual_information".into(),
                "normalized_mutual_information".into(),
            ],
        },
        Capability {
            name: "functional_dependency".into(),
            cost: "medium".into(),
            flag: Some("--measures functional_dependency".into()),
            returns: vec![
                "functional_dependency_left".into(),
                "functional_dependency_right".into(),
            ],
        },
        Capability {
            name: "null_cooccurrence".into(),
            cost: "medium".into(),
            flag: Some("--measures null_cooccurrence".into()),
            returns: vec!["null_both".into(), "null_jaccard".into()],
        },
        Capability {
            name: "numeric_relationship".into(),
            cost: "medium".into(),
            flag: Some("--measures numeric_relationship".into()),
            returns: vec![
                "pearson".into(),
                "spearman".into(),
                "same_sign_delta_fraction".into(),
            ],
        },
        Capability {
            name: "categorical_association".into(),
            cost: "medium".into(),
            flag: Some("--measures categorical_association".into()),
            returns: vec!["cramers_v".into()],
        },
    ]
}

fn explode_nested(dump: &Dump) -> Dump {
    let mut columns = Vec::new();
    let mut maps = Vec::new();
    for row in &dump.rows {
        let mut map = BTreeMap::new();
        for (name, value) in dump.columns.iter().zip(row.iter()) {
            extend_flat(&mut map, name, value);
        }
        for key in map.keys() {
            if !columns.iter().any(|column| column == key) {
                columns.push(key.clone());
            }
        }
        maps.push(map);
    }
    let rows = maps
        .iter()
        .map(|map| {
            columns
                .iter()
                .map(|column| map.get(column).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    Dump { columns, rows }
}

fn extend_flat(map: &mut BTreeMap<String, Value>, prefix: &str, value: &Value) {
    match value {
        Value::Object(fields) if !fields.is_empty() && fields.len() <= 32 => {
            for (key, child) in fields {
                extend_flat(map, &format!("{prefix}.{key}"), child);
            }
        }
        Value::Object(fields) if !fields.is_empty() => {
            map.insert(
                format!("{prefix}.map_length"),
                Value::from(fields.len() as u64),
            );
        }
        Value::Array(items) => {
            map.insert(
                format!("{prefix}.list_length"),
                Value::from(items.len() as u64),
            );
            if let Some(first) = items.first() {
                extend_flat(map, &format!("{prefix}.first"), first);
            }
        }
        other => {
            map.insert(prefix.to_string(), other.clone());
        }
    }
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
    let singleton_count = counts.values().filter(|count| **count == 1).count() as u64;
    let mut ranked: Vec<_> = counts.into_iter().collect();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    let frequency = if ndv > 0 && ndv <= 32 {
        ranked
            .iter()
            .map(|(value, count)| ValueCount {
                value: value.clone(),
                count: *count,
            })
            .collect()
    } else {
        Vec::new()
    };
    ranked.truncate(top);
    let top_values = ranked
        .into_iter()
        .map(|(value, count)| ValueCount { value, count })
        .collect();

    let (min_value, max_value) = bounds(&numbers, &texts);
    let range = numeric_range(&numbers);
    let (mean, stddev) = moments(&numbers);
    let skew = skewness(&numbers, mean, stddev);
    let quantile_p50 = quantile(&numbers, 0.50);
    let quantile_p90 = quantile(&numbers, 0.90);
    let quantile_p99 = quantile(&numbers, 0.99);
    let adjacent_delta_mean = mean_of(&deltas);
    let adjacent_delta_p50 = quantile(&deltas, 0.50);
    let adjacent_delta_p90 = quantile(&deltas, 0.90);
    let adjacent_delta_p99 = quantile(&deltas, 0.99);
    let adjacent_delta_max = deltas.iter().copied().reduce(f64::max);
    let adjacent_delta_zero_fraction = if deltas.is_empty() {
        None
    } else {
        Some(fraction(
            deltas.iter().filter(|delta| **delta == 0.0).count() as u64,
            deltas.len() as u64,
        ))
    };
    let lengths: Vec<u64> = texts.iter().map(|text| text.len() as u64).collect();
    let alphabet = alphabet_stats(&texts);

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
        frequency,
        singleton_count,
        singleton_fraction: fraction(singleton_count, ndv),
        min_value,
        max_value,
        range,
        mean,
        stddev,
        skew,
        quantile_p50,
        quantile_p90,
        quantile_p99,
        adjacent_delta_mean,
        adjacent_delta_p50,
        adjacent_delta_p90,
        adjacent_delta_p99,
        adjacent_delta_max,
        adjacent_delta_zero_fraction,
        length_mean: mean_of(&lengths.iter().map(|n| *n as f64).collect::<Vec<_>>()),
        length_min: lengths.iter().copied().min(),
        length_p50: quantile_u64(&lengths, 0.50),
        length_p90: quantile_u64(&lengths, 0.90),
        length_p99: quantile_u64(&lengths, 0.99),
        length_max: lengths.iter().copied().max(),
        common_prefix_length: common_affix(&texts, true),
        common_suffix_length: common_affix(&texts, false),
        adjacent_prefix_mean: mean_of(
            &prefix_lengths.iter().map(|n| *n as f64).collect::<Vec<_>>(),
        ),
        unique_code_points: alphabet.as_ref().map(|stats| stats.unique_code_points),
        ascii_fraction: alphabet.as_ref().map(|stats| stats.ascii_fraction),
        digit_fraction: alphabet.as_ref().map(|stats| stats.digit_fraction),
        letter_fraction: alphabet.as_ref().map(|stats| stats.letter_fraction),
        hex_fraction: alphabet.as_ref().map(|stats| stats.hex_fraction),
        whitespace_fraction: alphabet.as_ref().map(|stats| stats.whitespace_fraction),
        adjacent_equal_fraction: fraction(adjacent_equal, adjacent_pairs),
        run_length_mean: mean_of(&run_lengths.iter().map(|n| *n as f64).collect::<Vec<_>>())
            .unwrap_or(0.0),
        run_length_p50: quantile_u64(&run_lengths, 0.50),
        run_length_p90: quantile_u64(&run_lengths, 0.90),
        run_length_p99: quantile_u64(&run_lengths, 0.99),
        run_length_max: run_lengths.iter().copied().max(),
        monotonic: monotonic(compared, rising, falling).to_string(),
        patterns: patterns(&texts, ndv, non_null),
    }
}

const MEASURE_NAMES: &[&str] = &[
    "pair_ndv",
    "entropy",
    "mutual_information",
    "functional_dependency",
    "null_cooccurrence",
    "numeric_relationship",
    "categorical_association",
];

const MAX_DEPENDENCY_COLUMNS: usize = 8;

struct MeasureSet {
    pair_ndv: bool,
    entropy: bool,
    mutual_information: bool,
    functional_dependency: bool,
    null_cooccurrence: bool,
    numeric_relationship: bool,
    categorical_association: bool,
}

impl MeasureSet {
    fn all() -> Self {
        Self {
            pair_ndv: true,
            entropy: true,
            mutual_information: true,
            functional_dependency: true,
            null_cooccurrence: true,
            numeric_relationship: true,
            categorical_association: true,
        }
    }

    fn parse(names: &[String]) -> Result<Self, Error> {
        if names.is_empty() || names.iter().any(|name| name == "all") {
            return Ok(Self::all());
        }
        let mut set = Self {
            pair_ndv: false,
            entropy: false,
            mutual_information: false,
            functional_dependency: false,
            null_cooccurrence: false,
            numeric_relationship: false,
            categorical_association: false,
        };
        for name in names {
            match name.as_str() {
                "pair_ndv" => set.pair_ndv = true,
                "entropy" => set.entropy = true,
                "mutual_information" => set.mutual_information = true,
                "functional_dependency" => set.functional_dependency = true,
                "null_cooccurrence" => set.null_cooccurrence = true,
                "numeric_relationship" => set.numeric_relationship = true,
                "categorical_association" => set.categorical_association = true,
                other => {
                    return Err(Error(format!(
                        "unknown locality measure `{other}`; expected {}",
                        MEASURE_NAMES.join(", ")
                    )));
                }
            }
        }
        Ok(set)
    }

    fn names(&self) -> Vec<String> {
        MEASURE_NAMES
            .iter()
            .filter(|name| match **name {
                "pair_ndv" => self.pair_ndv,
                "entropy" => self.entropy,
                "mutual_information" => self.mutual_information,
                "functional_dependency" => self.functional_dependency,
                "null_cooccurrence" => self.null_cooccurrence,
                "numeric_relationship" => self.numeric_relationship,
                "categorical_association" => self.categorical_association,
                _ => false,
            })
            .map(|name| (*name).to_string())
            .collect()
    }
}

fn locality_pairs(
    dump: &Dump,
    columns: &[ColumnProfile],
    selected: &[usize],
    request: &ProfileRequest,
) -> Result<(Vec<(usize, usize)>, Locality), Error> {
    let measures = MeasureSet::parse(&request.measures)?.names();
    if !request.pairs.is_empty() {
        let mut pairs = Vec::new();
        let mut names = Vec::new();
        for (left, right) in &request.pairs {
            let left_index = column_index(dump, left)?;
            let right_index = column_index(dump, right)?;
            if left_index == right_index {
                return Err(Error(format!(
                    "pair `{left},{right}` names the same column"
                )));
            }
            push_name(&mut names, &dump.columns[left_index]);
            push_name(&mut names, &dump.columns[right_index]);
            let pair = (left_index, right_index);
            if !pairs.iter().any(|&(left, right)| {
                left == pair.0 && right == pair.1 || left == pair.1 && right == pair.0
            }) {
                pairs.push(pair);
            }
        }
        return Ok((
            pairs,
            Locality {
                selection: "requested".into(),
                columns: names,
                measures,
            },
        ));
    }
    let indexes = if selected.len() <= MAX_DEPENDENCY_COLUMNS && !request.columns.is_empty() {
        selected.to_vec()
    } else {
        promising_columns(columns, selected, &request.masses)
    };
    let selection = if selected.len() <= MAX_DEPENDENCY_COLUMNS && !request.columns.is_empty() {
        "requested"
    } else {
        "promising"
    };
    let names = indexes
        .iter()
        .map(|&index| dump.columns[index].clone())
        .collect();
    let mut pairs = Vec::new();
    for (offset, &left) in indexes.iter().enumerate() {
        for &right in indexes.iter().skip(offset + 1) {
            pairs.push((left, right));
        }
    }
    Ok((
        pairs,
        Locality {
            selection: selection.into(),
            columns: names,
            measures,
        },
    ))
}

fn column_index(dump: &Dump, name: &str) -> Result<usize, Error> {
    dump.columns
        .iter()
        .position(|column| column == name)
        .ok_or_else(|| Error(format!("pair column `{name}` is not in the sample")))
}

fn push_name(names: &mut Vec<String>, name: &str) {
    if !names.iter().any(|column| column == name) {
        names.push(name.to_string());
    }
}

fn promising_columns(
    columns: &[ColumnProfile],
    selected: &[usize],
    masses: &BTreeMap<String, u64>,
) -> Vec<usize> {
    let max_mass = columns
        .iter()
        .filter_map(|column| masses.get(&column.column))
        .copied()
        .max()
        .unwrap_or(0);
    let mut ranked: Vec<(f64, usize)> = columns
        .iter()
        .zip(selected.iter().copied())
        .map(|(column, index)| {
            (
                promising_score(column, masses.get(&column.column).copied(), max_mass),
                index,
            )
        })
        .filter(|(score, _)| *score > 0.0)
        .collect();
    ranked.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    ranked.truncate(MAX_DEPENDENCY_COLUMNS);
    if ranked.len() < 2 {
        return selected.iter().copied().take(2).collect();
    }
    ranked.into_iter().map(|(_, index)| index).collect()
}

fn promising_score(column: &ColumnProfile, mass: Option<u64>, max_mass: u64) -> f64 {
    let mass_score = match (mass, max_mass) {
        (Some(bytes), max) if max > 0 => (bytes as f64).ln_1p() / (max as f64).ln_1p(),
        _ => 0.0,
    };
    let enum_like = column.patterns.iter().any(|pattern| pattern == "enum_like")
        || (column.ndv > 1 && column.ndv <= 32 && column.ndv_ratio < 0.25);
    let numeric =
        matches!(column.physical_kind.as_str(), "INT" | "FLOAT") && column.range.is_some();
    if column.ndv_ratio > 0.9 && column.ndv > 16 && mass_score < 0.8 {
        return 0.0;
    }
    mass_score * 2.0
        + (column.entropy / 8.0).min(1.0)
        + if enum_like { 1.0 } else { 0.0 }
        + column.adjacent_equal_fraction
        + if column.null_fraction > 0.05 {
            column.null_fraction
        } else {
            0.0
        }
        + if numeric { 1.0 } else { 0.0 }
}

fn pair_dependency(
    left_name: &str,
    right_name: &str,
    left: &[&Value],
    right: &[&Value],
    measures: &MeasureSet,
) -> DependencyProfile {
    let mut left_counts = BTreeMap::new();
    let mut right_counts = BTreeMap::new();
    let mut pair_counts = BTreeMap::new();
    let mut null_both = 0u64;
    let mut null_left_only = 0u64;
    let mut null_right_only = 0u64;
    let mut null_neither = 0u64;
    let mut paired_left = Vec::new();
    let mut paired_right = Vec::new();
    let mut previous: Option<(f64, f64)> = None;
    let mut same_sign = 0u64;
    let mut signed_pairs = 0u64;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        match (left_value.is_null(), right_value.is_null()) {
            (true, true) => null_both += 1,
            (true, false) => null_left_only += 1,
            (false, true) => null_right_only += 1,
            (false, false) => null_neither += 1,
        }
        let left_key = value_key(left_value);
        let right_key = value_key(right_value);
        *left_counts.entry(left_key.clone()).or_insert(0) += 1;
        *right_counts.entry(right_key.clone()).or_insert(0) += 1;
        *pair_counts.entry((left_key, right_key)).or_insert(0) += 1;
        if measures.numeric_relationship {
            if let (Some(left_number), Some(right_number)) =
                (as_number(left_value), as_number(right_value))
            {
                paired_left.push(left_number);
                paired_right.push(right_number);
                if let Some((last_left, last_right)) = previous {
                    let left_delta = left_number - last_left;
                    let right_delta = right_number - last_right;
                    if left_delta != 0.0 && right_delta != 0.0 {
                        signed_pairs += 1;
                        if left_delta.signum() == right_delta.signum() {
                            same_sign += 1;
                        }
                    }
                }
                previous = Some((left_number, right_number));
            } else {
                previous = None;
            }
        }
    }
    let entropy_left = shannon(&left_counts.values().copied().collect::<Vec<_>>());
    let entropy_right = shannon(&right_counts.values().copied().collect::<Vec<_>>());
    let entropy_pair = shannon(&pair_counts.values().copied().collect::<Vec<_>>());
    let given_left = entropy_pair - entropy_left;
    let given_right = entropy_pair - entropy_right;
    let mutual = (entropy_left + entropy_right - entropy_pair).max(0.0);
    let denom = entropy_left + entropy_right;
    let (ndv_right_given_left_mean, ndv_right_given_left_max) = conditional_ndv(&pair_counts, true);
    let (ndv_left_given_right_mean, ndv_left_given_right_max) =
        conditional_ndv(&pair_counts, false);
    let null_union = null_both + null_left_only + null_right_only;
    DependencyProfile {
        left: left_name.to_string(),
        right: right_name.to_string(),
        ndv_left: maybe(measures.pair_ndv, left_counts.len() as u64),
        ndv_right: maybe(measures.pair_ndv, right_counts.len() as u64),
        ndv_pair: maybe(measures.pair_ndv, pair_counts.len() as u64),
        ndv_right_given_left_mean: maybe(measures.pair_ndv, ndv_right_given_left_mean),
        ndv_right_given_left_max: maybe(measures.pair_ndv, ndv_right_given_left_max),
        ndv_left_given_right_mean: maybe(measures.pair_ndv, ndv_left_given_right_mean),
        ndv_left_given_right_max: maybe(measures.pair_ndv, ndv_left_given_right_max),
        entropy_left: maybe(measures.entropy, entropy_left),
        entropy_right: maybe(measures.entropy, entropy_right),
        entropy_pair: maybe(measures.entropy, entropy_pair),
        entropy_right_given_left: maybe(measures.entropy, given_left.max(0.0)),
        entropy_left_given_right: maybe(measures.entropy, given_right.max(0.0)),
        mutual_information: maybe(measures.mutual_information, mutual),
        normalized_mutual_information: maybe(
            measures.mutual_information,
            if denom > 0.0 {
                2.0 * mutual / denom
            } else {
                0.0
            },
        ),
        functional_dependency_right: maybe(
            measures.functional_dependency,
            functional_dependency(entropy_right, given_left),
        ),
        functional_dependency_left: maybe(
            measures.functional_dependency,
            functional_dependency(entropy_left, given_right),
        ),
        null_both: maybe(measures.null_cooccurrence, null_both),
        null_left_only: maybe(measures.null_cooccurrence, null_left_only),
        null_right_only: maybe(measures.null_cooccurrence, null_right_only),
        null_neither: maybe(measures.null_cooccurrence, null_neither),
        null_jaccard: maybe(measures.null_cooccurrence, fraction(null_both, null_union)),
        pearson: if measures.numeric_relationship {
            pearson(&paired_left, &paired_right)
        } else {
            None
        },
        spearman: if measures.numeric_relationship {
            spearman(&paired_left, &paired_right)
        } else {
            None
        },
        same_sign_delta_fraction: maybe(
            measures.numeric_relationship && signed_pairs > 0,
            fraction(same_sign, signed_pairs),
        ),
        cramers_v: if measures.categorical_association {
            cramers_v(&left_counts, &right_counts, &pair_counts)
        } else {
            None
        },
    }
}

fn maybe<T>(want: bool, value: T) -> Option<T> {
    want.then_some(value)
}

fn conditional_ndv(
    pair_counts: &BTreeMap<(String, String), u64>,
    right_given_left: bool,
) -> (f64, u64) {
    let mut groups: BTreeMap<&str, BTreeMap<&str, u64>> = BTreeMap::new();
    for (left, right) in pair_counts.keys() {
        let (key, value) = if right_given_left {
            (left.as_str(), right.as_str())
        } else {
            (right.as_str(), left.as_str())
        };
        groups.entry(key).or_default().insert(value, 1);
    }
    if groups.is_empty() {
        return (0.0, 0);
    }
    let max = groups
        .values()
        .map(|values| values.len() as u64)
        .max()
        .unwrap_or(0);
    let mean = groups
        .values()
        .map(|values| values.len() as f64)
        .sum::<f64>()
        / groups.len() as f64;
    (mean, max)
}

fn pearson(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() < 3 || left.len() != right.len() {
        return None;
    }
    let n = left.len() as f64;
    let mean_left = left.iter().sum::<f64>() / n;
    let mean_right = right.iter().sum::<f64>() / n;
    let mut cov = 0.0;
    let mut var_left = 0.0;
    let mut var_right = 0.0;
    for (x, y) in left.iter().zip(right.iter()) {
        let dx = x - mean_left;
        let dy = y - mean_right;
        cov += dx * dy;
        var_left += dx * dx;
        var_right += dy * dy;
    }
    let denom = (var_left * var_right).sqrt();
    if denom == 0.0 {
        return None;
    }
    Some(cov / denom)
}

fn spearman(left: &[f64], right: &[f64]) -> Option<f64> {
    pearson(&ranks(left), &ranks(right))
}

fn ranks(values: &[f64]) -> Vec<f64> {
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&left, &right| {
        values[left]
            .partial_cmp(&values[right])
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut ranks = vec![0.0; values.len()];
    let mut index = 0;
    while index < order.len() {
        let mut last = index;
        while last + 1 < order.len()
            && values[order[last + 1]]
                .partial_cmp(&values[order[index]])
                .is_some_and(|order| order == std::cmp::Ordering::Equal)
        {
            last += 1;
        }
        let rank = (index + last) as f64 / 2.0 + 1.0;
        for slot in &order[index..=last] {
            ranks[*slot] = rank;
        }
        index = last + 1;
    }
    ranks
}

fn cramers_v(
    left_counts: &BTreeMap<String, u64>,
    right_counts: &BTreeMap<String, u64>,
    pair_counts: &BTreeMap<(String, String), u64>,
) -> Option<f64> {
    let rows = left_counts.len();
    let cols = right_counts.len();
    if rows < 2 || cols < 2 {
        return None;
    }
    let n = pair_counts.values().sum::<u64>() as f64;
    if n == 0.0 {
        return None;
    }
    let mut chi = 0.0;
    for ((left, right), observed) in pair_counts {
        let expected = *left_counts.get(left).unwrap_or(&0) as f64
            * *right_counts.get(right).unwrap_or(&0) as f64
            / n;
        if expected > 0.0 {
            let delta = *observed as f64 - expected;
            chi += delta * delta / expected;
        }
    }
    let denom = n * ((rows.min(cols) - 1) as f64);
    if denom == 0.0 {
        return None;
    }
    Some((chi / denom).sqrt().min(1.0))
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

fn numeric_range(numbers: &[f64]) -> Option<f64> {
    let min = numbers.iter().copied().reduce(f64::min)?;
    let max = numbers.iter().copied().reduce(f64::max)?;
    Some(max - min)
}

fn skewness(numbers: &[f64], mean: Option<f64>, stddev: Option<f64>) -> Option<f64> {
    let mean = mean?;
    let stddev = stddev?;
    if numbers.len() < 3 || stddev == 0.0 {
        return None;
    }
    let moment = numbers
        .iter()
        .map(|value| {
            let z = (value - mean) / stddev;
            z * z * z
        })
        .sum::<f64>()
        / numbers.len() as f64;
    Some(moment)
}

struct AlphabetStats {
    unique_code_points: u64,
    ascii_fraction: f64,
    digit_fraction: f64,
    letter_fraction: f64,
    hex_fraction: f64,
    whitespace_fraction: f64,
}

fn alphabet_stats(texts: &[&str]) -> Option<AlphabetStats> {
    if texts.is_empty() {
        return None;
    }
    let mut unique = BTreeMap::new();
    let mut bytes = 0u64;
    let mut ascii = 0u64;
    let mut digit = 0u64;
    let mut letter = 0u64;
    let mut hex = 0u64;
    let mut whitespace = 0u64;
    for text in texts.iter().take(256) {
        for ch in text.chars().take(256) {
            if unique.len() < 1024 {
                *unique.entry(ch).or_insert(0u64) += 1;
            }
            bytes += 1;
            if ch.is_ascii() {
                ascii += 1;
            }
            if ch.is_ascii_digit() {
                digit += 1;
            }
            if ch.is_ascii_alphabetic() {
                letter += 1;
            }
            if ch.is_ascii_hexdigit() {
                hex += 1;
            }
            if ch.is_ascii_whitespace() {
                whitespace += 1;
            }
        }
    }
    Some(AlphabetStats {
        unique_code_points: unique.len() as u64,
        ascii_fraction: fraction(ascii, bytes),
        digit_fraction: fraction(digit, bytes),
        letter_fraction: fraction(letter, bytes),
        hex_fraction: fraction(hex, bytes),
        whitespace_fraction: fraction(whitespace, bytes),
    })
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
            .any(|capability| capability.name == "locality" && capability.flag.is_some()));
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
        assert!(profile.dependencies[0].functional_dependency_right.unwrap() > 0.9);
        assert!(profile.dependencies[0].entropy_right_given_left.is_some());
        assert!(profile.dependencies[0].cramers_v.is_some());
        assert!(profile.locality.is_some());
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
        assert!(!country.frequency.is_empty());
        assert!(country.ascii_fraction.is_some());
        assert!(id.hex_fraction.unwrap() > 0.5);
    }

    #[test]
    fn explodes_nested_structs_and_lists() {
        let dump = dump(
            &["user", "tags"],
            vec![
                vec![json!({"city": "Berlin", "zip": 10115}), json!(["a", "b"])],
                vec![json!({"city": "Munich", "zip": 80331}), json!(["a"])],
            ],
        );
        let profile = profile(&dump, &ProfileRequest::default()).unwrap();
        let names: Vec<_> = profile
            .columns
            .iter()
            .map(|column| column.column.as_str())
            .collect();
        assert!(names.contains(&"user.city"));
        assert!(names.contains(&"user.zip"));
        assert!(names.contains(&"tags.list_length"));
        assert!(names.contains(&"tags.first"));
        let zip = profile
            .columns
            .iter()
            .find(|column| column.column == "user.zip")
            .unwrap();
        assert_eq!(zip.physical_kind, "INT");
        assert_eq!(zip.range, Some(70216.0));
        assert!(zip.adjacent_delta_p50.is_some());
    }

    #[test]
    fn requested_pairs_and_measures_are_honored() {
        let sample = dump(
            &["country", "city", "id"],
            vec![
                vec![json!("US"), json!("NYC"), json!(1)],
                vec![json!("US"), json!("NYC"), json!(2)],
                vec![json!("DE"), json!("BER"), json!(3)],
                vec![json!("DE"), json!("MUC"), json!(4)],
            ],
        );
        let profile = profile(
            &sample,
            &ProfileRequest {
                pairs: vec![("country".into(), "city".into())],
                measures: vec!["functional_dependency".into(), "pair_ndv".into()],
                ..ProfileRequest::default()
            },
        )
        .unwrap();
        assert_eq!(profile.dependencies.len(), 1);
        let pair = &profile.dependencies[0];
        assert_eq!(pair.left, "country");
        assert_eq!(pair.right, "city");
        assert!(pair.functional_dependency_left.unwrap() > 0.9);
        assert!(pair.ndv_pair.is_some());
        assert!(pair.mutual_information.is_none());
        assert!(pair.null_both.is_none());
        assert_eq!(profile.locality.as_ref().unwrap().selection, "requested");
    }

    #[test]
    fn null_and_numeric_measures_are_requestable() {
        let sample = dump(
            &["flag", "amount", "qty"],
            vec![
                vec![Value::Null, json!(1.0), json!(2.0)],
                vec![Value::Null, json!(2.0), json!(4.0)],
                vec![json!(true), json!(3.0), json!(6.0)],
                vec![json!(true), json!(4.0), json!(8.0)],
            ],
        );
        let nulls = profile(
            &sample,
            &ProfileRequest {
                pairs: vec![("flag".into(), "amount".into())],
                measures: vec!["null_cooccurrence".into()],
                ..ProfileRequest::default()
            },
        )
        .unwrap();
        assert_eq!(nulls.dependencies[0].null_both, Some(0));
        assert!(nulls.dependencies[0].null_jaccard.is_some());
        assert!(nulls.dependencies[0].pearson.is_none());
        let numeric = profile(
            &sample,
            &ProfileRequest {
                pairs: vec![("amount".into(), "qty".into())],
                measures: vec!["numeric_relationship".into()],
                ..ProfileRequest::default()
            },
        )
        .unwrap();
        assert!((numeric.dependencies[0].pearson.unwrap() - 1.0).abs() < 1e-9);
        assert!((numeric.dependencies[0].spearman.unwrap() - 1.0).abs() < 1e-9);
        assert!(numeric.dependencies[0].same_sign_delta_fraction.unwrap() > 0.9);
    }

    #[test]
    fn promising_locality_skips_unique_ids() {
        let mut rows = Vec::new();
        for index in 0..24 {
            rows.push(vec![
                json!(index),
                json!(index + 100),
                json!(index + 200),
                json!(if index % 2 == 0 { "US" } else { "DE" }),
                json!(if index % 2 == 0 { "NYC" } else { "BER" }),
            ]);
        }
        let sample = dump(&["id_a", "id_b", "id_c", "country", "city"], rows);
        let profile = profile(
            &sample,
            &ProfileRequest {
                dependencies: true,
                ..ProfileRequest::default()
            },
        )
        .unwrap();
        let locality = profile.locality.as_ref().unwrap();
        assert_eq!(locality.selection, "promising");
        assert!(locality.columns.contains(&"country".to_string()));
        assert!(locality.columns.contains(&"city".to_string()));
        assert!(profile.dependencies.iter().any(|pair| {
            (pair.left == "city" && pair.right == "country")
                || (pair.left == "country" && pair.right == "city")
        }));
        assert!(profile.dependencies.len() < 10);
    }

    #[test]
    fn unknown_measure_is_an_error() {
        let sample = dump(&["a", "b"], vec![vec![json!(1), json!(2)]]);
        let error = profile(
            &sample,
            &ProfileRequest {
                measures: vec!["pearson".into()],
                ..ProfileRequest::default()
            },
        )
        .unwrap_err();
        assert!(error.0.contains("locality measure"), "{error}");
    }
}
