//! `experiment`: rewrite a decoded row sample and measure the result.
//!
//! The command is one function: [`experiment`] takes a
//! [`TypedSample`](crate::third_party::parquet::api::TypedSample) plus an
//! [`ExperimentRequest`] and writes each requested rewrite with
//! `write_parquet`, then reads the resulting bytes back. A control trial is
//! always measured first so a trial can be compared against the same writer
//! without the requested change. Unlike `profile`, this does not recommend a
//! layout.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::Serialize;

use crate::third_party::parquet::api::{
    self, ColumnMass, FileMass, Kind, TypedColumn, TypedSample, Value, WriteOptions,
};

/// Rows per row group for skip-locality trials when none is requested.
const DEFAULT_ROW_GROUP_ROWS: usize = 2048;

/// Errors from the experiment layer: a bad aim, a bad rewrite, or a write
/// failure.
#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "experiment: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// What to measure after a rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aim {
    /// Compressed bytes and bytes per row.
    Storage,
    /// Row-group min/max locality (data skipping).
    Skipping,
    /// Both storage and skipping.
    All,
}

impl Aim {
    /// Parse `storage`, `skipping`, or `all`.
    ///
    /// # Errors
    /// Fails when the name is unknown.
    pub fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "storage" => Ok(Self::Storage),
            "skipping" => Ok(Self::Skipping),
            "all" => Ok(Self::All),
            other => Err(Error(format!(
                "unknown aim `{other}`; expected storage, skipping, or all"
            ))),
        }
    }

    /// The canonical name of this aim.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Storage => "storage",
            Self::Skipping => "skipping",
            Self::All => "all",
        }
    }

    fn skipping(self) -> bool {
        matches!(self, Self::Skipping | Self::All)
    }
}

/// Arguments for [`experiment`].
#[derive(Debug, Clone)]
pub struct ExperimentRequest {
    /// Rewrite specs to run. Empty runs only the control rewrite.
    pub trials: Vec<String>,
    /// What to measure.
    pub aim: Aim,
}

impl Default for ExperimentRequest {
    fn default() -> Self {
        Self {
            trials: Vec::new(),
            aim: Aim::Storage,
        }
    }
}

/// Empirical results: a control trial plus the requested trials.
#[derive(Debug, Clone)]
pub struct Experiment {
    /// Rows that were rewritten.
    pub row_count: u64,
    /// Actions this command can take.
    pub capabilities: Vec<Capability>,
    /// What was measured.
    pub aim: Aim,
    /// One row per trial, control first.
    pub trials: Vec<Trial>,
}

/// One action an agent can request: `aim` or `rewrite`.
#[derive(Debug, Clone, Serialize)]
pub struct Capability {
    /// Action name (`rewrite`, `aim`).
    pub name: String,
    /// `medium` (decode + write).
    pub cost: String,
    /// CLI flag that enables this action.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flag: Option<String>,
    /// Fact or rewrite names this action accepts.
    pub returns: Vec<String>,
}

/// The rewrites and aims this command supports.
fn capabilities() -> Vec<Capability> {
    vec![
        Capability {
            name: "aim".into(),
            cost: "medium".into(),
            flag: Some("--aim storage|skipping|all".into()),
            returns: vec!["storage".into(), "skipping".into(), "all".into()],
        },
        Capability {
            name: "rewrite".into(),
            cost: "medium".into(),
            flag: Some("--rewrite SPEC".into()),
            returns: vec![
                "sort:A,B".into(),
                "zorder:A,B".into(),
                "hilbert:A,B".into(),
                "codec:zstd@3".into(),
                "dictionary:on|off|BYTES".into(),
                "encoding:plain|delta|rle|delta_length|delta_byte_array|byte_stream_split".into(),
                "page-size:BYTES".into(),
                "row-group-size:ROWS".into(),
                "cast:COL:int64|double|string".into(),
            ],
        },
    ]
}

/// One rewrite and its measured file.
#[derive(Debug, Clone, Serialize)]
pub struct Trial {
    /// `control` or the requested spec.
    pub name: String,
    /// On-disk bytes of the rewritten file.
    pub bytes: u64,
    /// `bytes / row_count`.
    pub bytes_per_row: f64,
    /// Row groups in the rewritten file.
    pub row_group_count: u64,
    /// `bytes / control.bytes`, absent on the control trial.
    pub ratio: Option<f64>,
    /// Per-column storage and skip facts.
    pub columns: Vec<ColumnTrial>,
}

/// Facts for one column in one trial.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnTrial {
    /// Column name.
    pub column: String,
    /// Compressed bytes summed across row groups.
    pub compressed_bytes: u64,
    /// `compressed_bytes / row_count`.
    pub bytes_per_row: f64,
    /// Compression codec recorded on the first chunk.
    pub codec: String,
    /// Encodings used by the column, in first-seen order.
    pub encodings: Vec<String>,
    /// A dictionary page offset is present.
    pub dictionary: bool,
    /// Mean `(row-group span / file span)`, when numeric min/max exist.
    pub skip_span_ratio: Option<f64>,
    /// Fraction of row groups whose min equals max.
    pub skip_point_equal_fraction: Option<f64>,
}

/// Rewrite a decoded sample and measure each requested trial against a control.
///
/// # Errors
/// Fails when a rewrite spec is unknown or names a missing column, when the
/// sample is empty, or when the writer cannot emit Parquet.
pub fn experiment(sample: &TypedSample, request: &ExperimentRequest) -> Result<Experiment, Error> {
    if sample.rows.is_empty() {
        return Err(Error("sample has no rows".into()));
    }
    let names: Vec<String> = sample.columns.iter().map(|c| c.name.clone()).collect();
    let parsed: Vec<TrialSpec> = request
        .trials
        .iter()
        .map(|spec| TrialSpec::parse(spec, &names))
        .collect::<Result<_, _>>()?;
    let row_count = sample.rows.len() as u64;
    let control_spec = TrialSpec::control();
    let control_bytes = write_trial(sample, &control_spec, request.aim)?;
    let control = measure(&control_spec, &control_bytes, request.aim, None, row_count)?;
    let mut trials = vec![control.clone()];
    for spec in parsed {
        let bytes = write_trial(sample, &spec, request.aim)?;
        trials.push(measure(
            &spec,
            &bytes,
            request.aim,
            Some(&control),
            row_count,
        )?);
    }
    Ok(Experiment {
        row_count,
        capabilities: capabilities(),
        aim: request.aim,
        trials,
    })
}

/// A row-ordering rewrite.
#[derive(Debug, Clone)]
enum Layout {
    Keep,
    Sort(Vec<String>),
    ZOrder(Vec<String>),
    Hilbert(Vec<String>),
}

/// A requested type change for one column.
// aipnaming: allow(aip-136/method-prepositions)
#[derive(Debug, Clone, Copy)]
enum CastTo {
    Int64,
    Double,
    String,
}

impl CastTo {
    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "int64" => Ok(Self::Int64),
            "double" => Ok(Self::Double),
            "string" => Ok(Self::String),
            other => Err(Error(format!(
                "cast target `{other}` must be int64, double, or string"
            ))),
        }
    }
}

/// One rewrite request, parsed from a semicolon-composed spec.
#[derive(Debug, Clone)]
struct TrialSpec {
    name: String,
    rewrites: Vec<String>,
    layout: Layout,
    codec: String,
    level: Option<i32>,
    dictionary: bool,
    dictionary_bytes: Option<usize>,
    encoding: Option<String>,
    page_size: Option<usize>,
    casts: Vec<(String, CastTo)>,
    row_group_size: Option<usize>,
}

impl TrialSpec {
    fn control() -> Self {
        Self {
            name: "control".into(),
            rewrites: Vec::new(),
            layout: Layout::Keep,
            codec: "zstd".into(),
            level: None,
            dictionary: true,
            dictionary_bytes: None,
            encoding: None,
            page_size: None,
            casts: Vec::new(),
            row_group_size: None,
        }
    }

    fn parse(source: &str, columns: &[String]) -> Result<Self, Error> {
        let mut spec = Self {
            name: source.to_string(),
            ..Self::control()
        };
        for part in source.split(';') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            spec.rewrites.push(part.to_string());
            spec.apply(part, columns)?;
        }
        if spec.rewrites.is_empty() {
            return Err(Error("trial has no rewrites".into()));
        }
        Ok(spec)
    }

    fn apply(&mut self, part: &str, columns: &[String]) -> Result<(), Error> {
        let (key, value) = part.split_once(':').ok_or_else(|| {
            Error(format!(
                "rewrite `{part}` must be key:value (sort, zorder, hilbert, codec, dictionary, \
                 encoding, page-size, row-group-size, cast)"
            ))
        })?;
        match key {
            "sort" => self.layout = Layout::Sort(column_list(value, columns)?),
            "zorder" => self.layout = Layout::ZOrder(column_list(value, columns)?),
            "hilbert" => {
                let names = column_list(value, columns)?;
                if names.len() != 2 {
                    return Err(Error(
                        "hilbert needs exactly two columns; use zorder for more".into(),
                    ));
                }
                self.layout = Layout::Hilbert(names);
            }
            "codec" => {
                let (codec, level) = parse_codec(value)?;
                self.codec = codec;
                self.level = level;
            }
            "dictionary" => match value {
                "on" => self.dictionary = true,
                "off" => self.dictionary = false,
                bytes => {
                    self.dictionary = true;
                    self.dictionary_bytes = Some(parse_usize(bytes, "dictionary")?);
                }
            },
            "encoding" => self.encoding = Some(parse_encoding(value)?.to_string()),
            "page-size" => self.page_size = Some(parse_usize(value, "page-size")?),
            "row-group-size" => self.row_group_size = Some(parse_usize(value, "row-group-size")?),
            "cast" => self.casts.push(parse_cast(value, columns)?),
            other => {
                return Err(Error(format!(
                    "unknown rewrite `{other}`; expected sort, zorder, hilbert, codec, dictionary, \
                     encoding, page-size, row-group-size, cast"
                )));
            }
        }
        Ok(())
    }

    fn row_group_rows(&self, rows: usize, aim: Aim) -> usize {
        if let Some(size) = self.row_group_size {
            return size;
        }
        if aim.skipping() {
            DEFAULT_ROW_GROUP_ROWS
        } else {
            rows.max(1)
        }
    }
}

/// Parse a positive integer option.
fn parse_usize(value: &str, name: &str) -> Result<usize, Error> {
    let number: usize = value
        .parse()
        .map_err(|_| Error(format!("{name} `{value}` is not a positive integer")))?;
    if number == 0 {
        return Err(Error(format!("{name} must be >= 1")));
    }
    Ok(number)
}

/// Parse `COL:TYPE` into a column and cast target, requiring the column.
fn parse_cast(value: &str, columns: &[String]) -> Result<(String, CastTo), Error> {
    let (column, target) = value
        .split_once(':')
        .ok_or_else(|| Error(format!("cast `{value}` must be COL:int64|double|string")))?;
    let column = require_column(column.trim(), columns)?;
    Ok((column, CastTo::parse(target.trim())?))
}

/// Require a column name to exist in the sample.
fn require_column(name: &str, columns: &[String]) -> Result<String, Error> {
    columns
        .iter()
        .find(|column| column.as_str() == name)
        .cloned()
        .ok_or_else(|| Error(format!("column `{name}` is not in the sample")))
}

/// Parse an encoding name, returning it unchanged when known.
fn parse_encoding(value: &str) -> Result<&str, Error> {
    match value {
        "plain" | "delta" | "rle" | "delta_length" | "delta_byte_array" | "byte_stream_split" => {
            Ok(value)
        }
        other => Err(Error(format!(
            "unknown encoding `{other}`; expected plain, delta, rle, delta_length, \
             delta_byte_array, byte_stream_split"
        ))),
    }
}

/// Parse a comma-separated column list, requiring every name to exist.
fn column_list(value: &str, columns: &[String]) -> Result<Vec<String>, Error> {
    let names: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| {
            columns
                .iter()
                .find(|column| column.as_str() == name)
                .cloned()
                .ok_or_else(|| Error(format!("column `{name}` is not in the sample")))
        })
        .collect::<Result<_, _>>()?;
    if names.is_empty() {
        return Err(Error("sort needs at least one column".into()));
    }
    Ok(names)
}

/// Parse `NAME` or `NAME@LEVEL`.
fn parse_codec(value: &str) -> Result<(String, Option<i32>), Error> {
    let (name, level) = match value.split_once('@') {
        Some((name, level)) => {
            let level: i32 = level
                .parse()
                .map_err(|_| Error(format!("codec level `{level}` is not an integer")))?;
            (name, Some(level))
        }
        None => (value, None),
    };
    match name {
        "uncompressed" | "none" | "snappy" | "gzip" | "lz4" | "zstd" => {
            Ok((name.to_string(), level))
        }
        other => Err(Error(format!(
            "unknown codec `{other}`; expected uncompressed, snappy, gzip, lz4, zstd"
        ))),
    }
}

/// Write one trial's bytes (casting and reordering a copy of the rows first).
fn write_trial(sample: &TypedSample, spec: &TrialSpec, aim: Aim) -> Result<Vec<u8>, Error> {
    let mut columns = sample.columns.clone();
    let mut rows = sample.rows.clone();
    apply_casts(&mut columns, &mut rows, &spec.casts)?;
    apply_layout(&mut rows, &columns, &spec.layout)?;
    let sorting_columns = match &spec.layout {
        Layout::Sort(names) => names.clone(),
        _ => Vec::new(),
    };
    let options = WriteOptions {
        compression: spec.codec.clone(),
        level: spec.level,
        dictionary: spec.dictionary,
        dictionary_bytes: spec.dictionary_bytes,
        encoding: spec.encoding.clone(),
        page_size: spec.page_size,
        sorting_columns,
        row_group_size: spec.row_group_rows(sample.rows.len(), aim),
    };
    api::write_parquet(&columns, &rows, &options).map_err(|error| Error(error.to_string()))
}

/// Apply each cast to the column kinds and every row's value.
fn apply_casts(
    columns: &mut [TypedColumn],
    rows: &mut [Vec<Value>],
    casts: &[(String, CastTo)],
) -> Result<(), Error> {
    for (name, target) in casts {
        let index = columns
            .iter()
            .position(|column| &column.name == name)
            .ok_or_else(|| Error(format!("column `{name}` is not in the sample")))?;
        columns[index].kind = match target {
            CastTo::Int64 => Kind::Integer,
            CastTo::Double => Kind::Number,
            CastTo::String => Kind::Bytes,
        };
        for row in rows.iter_mut() {
            let value = std::mem::replace(&mut row[index], Value::Null);
            row[index] = cast_value(&value, *target, name)?;
        }
    }
    Ok(())
}

/// Cast one value to the target type; nulls pass through.
fn cast_value(value: &Value, to: CastTo, name: &str) -> Result<Value, Error> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    match to {
        CastTo::Int64 => match value {
            Value::Integer(number) => Ok(Value::Integer(*number)),
            Value::Number(number) if number.fract() == 0.0 => Ok(Value::Integer(*number as i64)),
            Value::Bytes(bytes) => text(bytes)
                .trim()
                .parse()
                .map(Value::Integer)
                .map_err(|_| Error(format!("cannot cast `{name}` to int64"))),
            _ => Err(Error(format!("cannot cast `{name}` to int64"))),
        },
        CastTo::Double => match value {
            Value::Number(number) => Ok(Value::Number(*number)),
            Value::Integer(number) => Ok(Value::Number(*number as f64)),
            Value::Bytes(bytes) => text(bytes)
                .trim()
                .parse()
                .map(Value::Number)
                .map_err(|_| Error(format!("cannot cast `{name}` to double"))),
            _ => Err(Error(format!("cannot cast `{name}` to double"))),
        },
        CastTo::String => Ok(match value {
            Value::Bytes(bytes) => Value::Bytes(bytes.clone()),
            other => Value::Bytes(value_text(other).into_bytes()),
        }),
    }
}

/// Decode bytes as UTF-8, replacing invalid sequences.
fn text(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(bytes)
}

/// Reorder rows in place by the requested layout.
fn apply_layout(
    rows: &mut Vec<Vec<Value>>,
    columns: &[TypedColumn],
    layout: &Layout,
) -> Result<(), Error> {
    match layout {
        Layout::Keep => Ok(()),
        Layout::Sort(names) => {
            let indexes = column_indexes(columns, names)?;
            rows.sort_by(|left, right| compare_rows(left, right, &indexes));
            Ok(())
        }
        Layout::ZOrder(names) => {
            let keys = space_keys(rows, columns, names, Space::ZOrder)?;
            reorder(rows, &keys);
            Ok(())
        }
        Layout::Hilbert(names) => {
            let keys = space_keys(rows, columns, names, Space::Hilbert)?;
            reorder(rows, &keys);
            Ok(())
        }
    }
}

/// Resolve column names to their indexes in order.
fn column_indexes(columns: &[TypedColumn], names: &[String]) -> Result<Vec<usize>, Error> {
    names
        .iter()
        .map(|name| {
            columns
                .iter()
                .position(|column| &column.name == name)
                .ok_or_else(|| Error(format!("column `{name}` is not in the sample")))
        })
        .collect()
}

fn compare_rows(left: &[Value], right: &[Value], indexes: &[usize]) -> Ordering {
    for &index in indexes {
        let order = compare_values(
            left.get(index).unwrap_or(&Value::Null),
            right.get(index).unwrap_or(&Value::Null),
        );
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

/// Compare two typed values; nulls sort last, numerics compare numerically.
fn compare_values(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Bytes(left), Value::Bytes(right)) => left.cmp(right),
        _ => match (value_number(left), value_number(right)) {
            (Some(left), Some(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
            _ => value_text(left).cmp(&value_text(right)),
        },
    }
}

fn value_number(value: &Value) -> Option<f64> {
    match value {
        Value::Integer(number) => Some(*number as f64),
        Value::Number(number) => Some(*number),
        _ => None,
    }
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Boolean(flag) => flag.to_string(),
        Value::Integer(number) => number.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

/// A stable key that groups values that compare equal.
fn value_key(value: &Value) -> String {
    match value {
        Value::Null => "n".to_string(),
        Value::Boolean(flag) => format!("b{flag}"),
        Value::Integer(number) => format!("i{number}"),
        Value::Number(number) => format!("f{number}"),
        Value::Bytes(bytes) => format!("s{}", String::from_utf8_lossy(bytes)),
    }
}

/// Morton or Hilbert space keying over ranked key columns.
#[derive(Clone, Copy)]
enum Space {
    ZOrder,
    Hilbert,
}

/// The u128 space key of every row for the requested curve.
fn space_keys(
    rows: &[Vec<Value>],
    columns: &[TypedColumn],
    names: &[String],
    space: Space,
) -> Result<Vec<u128>, Error> {
    let indexes = column_indexes(columns, names)?;
    let ranks: Vec<Vec<u32>> = indexes
        .iter()
        .map(|&index| column_ranks(rows, index))
        .collect();
    Ok((0..rows.len())
        .map(|row| match space {
            Space::ZOrder => zorder_key(ranks.iter().map(|column| column[row])),
            Space::Hilbert => hilbert_key(ranks[0][row], ranks[1][row]),
        })
        .collect())
}

/// Rank the values in one column, equal values sharing a rank.
fn column_ranks(rows: &[Vec<Value>], index: usize) -> Vec<u32> {
    let mut values: Vec<&Value> = rows.iter().map(|row| &row[index]).collect();
    values.sort_by(|left, right| compare_values(left, right));
    let mut ranks: BTreeMap<String, u32> = BTreeMap::new();
    let mut next = 0u32;
    let mut previous: Option<&Value> = None;
    for value in values {
        if previous.is_none_or(|previous| compare_values(previous, value) != Ordering::Equal) {
            ranks.insert(value_key(value), next);
            next += 1;
            previous = Some(value);
        }
    }
    rows.iter()
        .map(|row| *ranks.get(&value_key(&row[index])).unwrap_or(&0))
        .collect()
}

/// Interleave the low 16 bits of each rank (Morton order).
fn zorder_key(ranks: impl Iterator<Item = u32>) -> u128 {
    let ranks: Vec<u32> = ranks.collect();
    let mut key = 0u128;
    for bit in (0..16).rev() {
        for rank in &ranks {
            key = (key << 1) | u128::from((rank >> bit) & 1);
        }
    }
    key
}

/// The 2-D Hilbert curve index of two ranks.
fn hilbert_key(x: u32, y: u32) -> u128 {
    let mut x = x;
    let mut y = y;
    let mut index = 0u128;
    let mut scale = 1u32 << 15;
    while scale > 0 {
        let rx = u32::from(x & scale != 0);
        let ry = u32::from(y & scale != 0);
        index += u128::from(scale) * u128::from(scale) * u128::from((3 * rx) ^ ry);
        rotate(scale, &mut x, &mut y, rx, ry);
        scale >>= 1;
    }
    index
}

fn rotate(scale: u32, x: &mut u32, y: &mut u32, rx: u32, ry: u32) {
    if ry != 0 {
        return;
    }
    if rx == 1 {
        *x = scale.saturating_sub(1).saturating_sub(*x);
        *y = scale.saturating_sub(1).saturating_sub(*y);
    }
    std::mem::swap(x, y);
}

/// Reorder rows in place by ascending space key.
fn reorder(rows: &mut Vec<Vec<Value>>, keys: &[u128]) {
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by_key(|&index| keys[index]);
    let sorted: Vec<Vec<Value>> = order
        .into_iter()
        .map(|index| std::mem::take(&mut rows[index]))
        .collect();
    *rows = sorted;
}

/// Measure one written trial against the control.
fn measure(
    spec: &TrialSpec,
    bytes: &[u8],
    aim: Aim,
    control: Option<&Trial>,
    row_count: u64,
) -> Result<Trial, Error> {
    let mass = api::read_buffer_masses(bytes, false).map_err(|error| Error(error.to_string()))?;
    let size = bytes.len() as u64;
    let ratio =
        control.and_then(|control| (control.bytes > 0).then(|| size as f64 / control.bytes as f64));
    Ok(Trial {
        name: spec.name.clone(),
        bytes: size,
        bytes_per_row: size as f64 / row_count.max(1) as f64,
        row_group_count: mass.row_group_count as u64,
        ratio,
        columns: column_trials(&mass, aim, row_count),
    })
}

/// Reduce a file mass into per-column trials.
fn column_trials(mass: &FileMass, aim: Aim, row_count: u64) -> Vec<ColumnTrial> {
    let mut grouped: BTreeMap<&str, Vec<&ColumnMass>> = BTreeMap::new();
    for column in &mass.columns {
        grouped
            .entry(column.column.as_str())
            .or_default()
            .push(column);
    }
    grouped
        .into_iter()
        .map(|(name, chunks)| {
            let compressed_bytes: u64 = chunks.iter().map(|chunk| chunk.compressed_bytes).sum();
            let first = chunks[0];
            let mut encodings = Vec::new();
            for chunk in &chunks {
                for encoding in &chunk.encodings {
                    if !encodings.contains(encoding) {
                        encodings.push(encoding.clone());
                    }
                }
            }
            let has_stats = chunks
                .iter()
                .all(|chunk| chunk.min_value.is_some() && chunk.max_value.is_some());
            ColumnTrial {
                column: name.to_string(),
                compressed_bytes,
                bytes_per_row: compressed_bytes as f64 / row_count.max(1) as f64,
                codec: first.codec.clone(),
                encodings,
                dictionary: first.dictionary,
                skip_span_ratio: if aim.skipping() && has_stats {
                    skip_span(&chunks)
                } else {
                    None
                },
                skip_point_equal_fraction: if aim.skipping() && has_stats {
                    Some(skip_equal(&chunks))
                } else {
                    None
                },
            }
        })
        .collect()
}

/// Mean `(row-group span / file span)`; `None` without numeric stats or a
/// zero global span.
fn skip_span(chunks: &[&ColumnMass]) -> Option<f64> {
    let mut spans = Vec::with_capacity(chunks.len());
    let mut global_min = f64::INFINITY;
    let mut global_max = f64::NEG_INFINITY;
    for chunk in chunks {
        let min: f64 = chunk.min_value.as_deref()?.parse().ok()?;
        let max: f64 = chunk.max_value.as_deref()?.parse().ok()?;
        global_min = global_min.min(min);
        global_max = global_max.max(max);
        spans.push(max - min);
    }
    let global = global_max - global_min;
    if global == 0.0 {
        return None;
    }
    Some(spans.iter().sum::<f64>() / spans.len() as f64 / global)
}

/// Fraction of row groups whose min equals max.
fn skip_equal(chunks: &[&ColumnMass]) -> f64 {
    if chunks.is_empty() {
        return 0.0;
    }
    let hits = chunks
        .iter()
        .filter(|chunk| chunk.min_value.is_some() && chunk.min_value == chunk.max_value)
        .count();
    hits as f64 / chunks.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::third_party::parquet::api::Kind;

    fn sample(columns: &[(&str, Kind)], rows: Vec<Vec<Value>>) -> TypedSample {
        TypedSample {
            columns: columns
                .iter()
                .map(|(name, kind)| TypedColumn {
                    name: (*name).to_string(),
                    kind: *kind,
                })
                .collect(),
            rows,
        }
    }

    #[test]
    fn control_is_always_measured() {
        let sample = sample(
            &[("n", Kind::Integer)],
            (0..32).map(|n| vec![Value::Integer(n)]).collect(),
        );
        let result = experiment(&sample, &ExperimentRequest::default()).unwrap();
        assert_eq!(result.trials.len(), 1);
        assert_eq!(result.trials[0].name, "control");
        assert_eq!(result.row_count, 32);
        assert!(result.trials[0].bytes > 0);
    }

    #[test]
    fn sort_improves_skip_span_on_a_shuffled_key() {
        let rows: Vec<Vec<Value>> = (0..4096)
            .map(|n| {
                vec![
                    Value::Integer((n * 7) % 4096),
                    Value::Bytes(format!("payload-{n}").into_bytes()),
                ]
            })
            .collect();
        let sample = sample(&[("id", Kind::Integer), ("payload", Kind::Bytes)], rows);
        let result = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["sort:id".into()],
                aim: Aim::Skipping,
            },
        )
        .unwrap();
        let span = |name: &str| {
            result
                .trials
                .iter()
                .find(|trial| trial.name == name)
                .unwrap()
                .columns
                .iter()
                .find(|column| column.column == "id")
                .unwrap()
                .skip_span_ratio
                .unwrap()
        };
        let control = span("control");
        let sorted = span("sort:id");
        assert!(
            sorted < control,
            "sorted span {sorted} should be tighter than control {control}"
        );
    }

    #[test]
    fn uncompressed_is_larger_than_zstd_on_repeated_text() {
        let rows = (0..256)
            .map(|_| vec![Value::Bytes(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_vec())])
            .collect();
        let sample = sample(&[("text", Kind::Bytes)], rows);
        let result = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["codec:uncompressed".into()],
                aim: Aim::Storage,
            },
        )
        .unwrap();
        let control = result.trials[0].bytes;
        let raw = result
            .trials
            .iter()
            .find(|trial| trial.name == "codec:uncompressed")
            .unwrap()
            .bytes;
        assert!(
            raw > control,
            "uncompressed {raw} should exceed zstd {control}"
        );
    }

    #[test]
    fn unknown_rewrite_is_an_error() {
        let sample = sample(&[("a", Kind::Integer)], vec![vec![Value::Integer(1)]]);
        let error = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["correlate:a".into()],
                ..ExperimentRequest::default()
            },
        )
        .unwrap_err();
        assert!(error.0.contains("rewrite"), "{error}");
    }

    #[test]
    fn semicolon_composition_works() {
        let rows = (0..64)
            .map(|n| vec![Value::Integer(n), Value::Bytes(b"x".to_vec())])
            .collect();
        let sample = sample(&[("a", Kind::Integer), ("b", Kind::Bytes)], rows);
        let result = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["codec:snappy;dictionary:off;row-group-size:16".into()],
                aim: Aim::Storage,
            },
        )
        .unwrap();
        let trial = result
            .trials
            .iter()
            .find(|trial| trial.name == "codec:snappy;dictionary:off;row-group-size:16")
            .unwrap();
        assert_eq!(trial.row_group_count, 4);
        assert!(trial.columns.iter().all(|column| !column.dictionary));
    }

    #[test]
    fn cast_value_converts_between_kinds() {
        assert!(matches!(
            cast_value(&Value::Bytes(b"7".to_vec()), CastTo::Int64, "c").unwrap(),
            Value::Integer(7)
        ));
        assert!(matches!(
            cast_value(&Value::Integer(5), CastTo::Double, "c").unwrap(),
            Value::Number(number) if number == 5.0
        ));
        assert!(matches!(
            cast_value(&Value::Integer(5), CastTo::String, "c").unwrap(),
            Value::Bytes(bytes) if bytes == b"5"
        ));
        assert!(cast_value(&Value::Bytes(b"x".to_vec()), CastTo::Int64, "c").is_err());
    }

    #[test]
    fn zorder_and_hilbert_are_accepted() {
        let rows: Vec<Vec<Value>> = (0..1024)
            .map(|n| {
                vec![
                    Value::Integer((n * 13) % 1024),
                    Value::Integer((n * 7) % 1024),
                ]
            })
            .collect();
        let sample = sample(&[("x", Kind::Integer), ("y", Kind::Integer)], rows);
        let result = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["zorder:x,y".into(), "hilbert:x,y".into()],
                aim: Aim::Skipping,
            },
        )
        .unwrap();
        assert_eq!(result.trials.len(), 3);
        for name in ["zorder:x,y", "hilbert:x,y"] {
            assert!(
                result.trials.iter().any(|trial| trial.name == name),
                "missing trial {name}"
            );
        }
    }

    #[test]
    fn hilbert_needs_exactly_two_columns() {
        let sample = sample(
            &[
                ("a", Kind::Integer),
                ("b", Kind::Integer),
                ("c", Kind::Integer),
            ],
            vec![vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]],
        );
        let error = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["hilbert:a,b,c".into()],
                ..ExperimentRequest::default()
            },
        )
        .unwrap_err();
        assert!(error.0.contains("two columns"), "{error}");
    }

    #[test]
    fn encoding_page_size_and_dictionary_bytes_are_accepted() {
        let sample = sample(
            &[("i", Kind::Integer)],
            (0..256).map(|n| vec![Value::Integer(n)]).collect(),
        );
        let result = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec![
                    "encoding:delta".into(),
                    "page-size:1024".into(),
                    "dictionary:64".into(),
                ],
                aim: Aim::Storage,
            },
        )
        .unwrap();
        assert_eq!(result.trials.len(), 4);
        let dictionary = result
            .trials
            .iter()
            .find(|trial| trial.name == "dictionary:64")
            .unwrap();
        assert!(dictionary.columns.iter().all(|column| column.dictionary));
    }

    #[test]
    fn encoding_is_type_aware_not_a_panic() {
        let bytes_only = sample(
            &[("text", Kind::Bytes)],
            vec![
                vec![Value::Bytes(b"a".to_vec())],
                vec![Value::Bytes(b"b".to_vec())],
            ],
        );
        let error = experiment(
            &bytes_only,
            &ExperimentRequest {
                trials: vec!["encoding:delta".into()],
                ..ExperimentRequest::default()
            },
        )
        .unwrap_err();
        assert!(error.0.contains("encod"), "{error}");

        let mixed = sample(
            &[("n", Kind::Integer), ("text", Kind::Bytes)],
            vec![vec![Value::Integer(1), Value::Bytes(b"a".to_vec())]],
        );
        assert!(experiment(
            &mixed,
            &ExperimentRequest {
                trials: vec!["encoding:delta".into()],
                ..ExperimentRequest::default()
            },
        )
        .is_ok());
    }

    #[test]
    fn capabilities_advertise_rewrites_and_aims() {
        let sample = sample(&[("a", Kind::Integer)], vec![vec![Value::Integer(1)]]);
        let result = experiment(&sample, &ExperimentRequest::default()).unwrap();
        let rewrite = result
            .capabilities
            .iter()
            .find(|capability| capability.name == "rewrite")
            .unwrap();
        for expected in ["sort", "zorder", "hilbert", "cast", "encoding", "page-size"] {
            assert!(
                rewrite
                    .returns
                    .iter()
                    .any(|item| item.starts_with(expected)),
                "capabilities missing {expected}"
            );
        }
        assert!(result.capabilities.iter().any(|c| c.name == "aim"));
    }
}
