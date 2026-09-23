//! Rewrite a dump sample and measure the result.
//!
//! [`experiment`] applies agent-requested rewrites (sort, codec, page size, …)
//! and reports empirical storage (bytes per row) and/or data-skipping facts.
//! It does not recommend a layout. Default work is one control rewrite so a
//! trial can be compared to the same writer without the requested change.

use std::collections::BTreeMap;
use std::sync::Arc;

use parquet::basic::{
    Compression, ConvertedType, Encoding, LogicalType, Repetition, Type as PhysicalType,
};
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;
use serde::Serialize;
use serde_json::Value;

use crate::dump::Dump;
use crate::parquet_helpers::{self, Error, FileMass};

/// What to measure after a rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aim {
    /// Compressed bytes and bytes per row.
    STORAGE,
    /// Row-group min/max locality (data skipping).
    SKIPPING,
    /// Both storage and skipping.
    ALL,
}

impl Aim {
    /// Parse `storage`, `skipping`, or `all`.
    ///
    /// # Errors
    /// Fails when the name is unknown.
    pub fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "storage" => Ok(Self::STORAGE),
            "skipping" => Ok(Self::SKIPPING),
            "all" => Ok(Self::ALL),
            other => Err(Error(format!(
                "unknown aim `{other}`; expected storage, skipping, or all"
            ))),
        }
    }

    fn storage(self) -> bool {
        matches!(self, Self::STORAGE | Self::ALL)
    }

    fn skipping(self) -> bool {
        matches!(self, Self::SKIPPING | Self::ALL)
    }
}

/// Arguments for [`experiment`].
#[derive(Debug, Clone)]
pub struct ExperimentRequest {
    /// Trials to run. Empty runs only the control rewrite.
    pub trials: Vec<String>,
    /// What to measure.
    pub aim: Aim,
    /// Also load page indexes from the rewritten footer.
    pub indexes: bool,
}

impl Default for ExperimentRequest {
    fn default() -> Self {
        Self {
            trials: Vec::new(),
            aim: Aim::STORAGE,
            indexes: false,
        }
    }
}

/// Empirical results: control plus requested trials.
#[derive(Debug, Clone, Serialize)]
pub struct Experiment {
    /// Rows that were rewritten.
    pub num_rows: u64,
    /// Actions this command can take.
    pub capabilities: Vec<Capability>,
    /// What was measured.
    pub aim: String,
    /// One row per trial, control first.
    pub trials: Vec<Trial>,
}

/// One measurement an agent can request.
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

/// One rewrite and its measured file.
#[derive(Debug, Clone, Serialize)]
pub struct Trial {
    /// `control` or the requested spec.
    pub name: String,
    /// Rewrites that were applied.
    pub rewrites: Vec<String>,
    /// On-disk bytes of the rewritten file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_bytes: Option<u64>,
    /// `file_bytes / num_rows`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_per_row: Option<f64>,
    /// `file_bytes / control.file_bytes`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_bytes_ratio: Option<f64>,
    /// Row groups in the rewritten file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_group_count: Option<u64>,
    /// Per-column storage and skip facts.
    pub columns: Vec<ColumnTrial>,
}

/// Facts for one column in one trial.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnTrial {
    /// Column name.
    pub column: String,
    /// Compressed bytes across row groups.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compressed_bytes: Option<u64>,
    /// `compressed_bytes / num_rows`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_per_row: Option<f64>,
    /// `compressed_bytes / control.compressed_bytes`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compressed_bytes_ratio: Option<f64>,
    /// Mean `(row-group range / file range)` when numeric min/max exist.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_span_ratio: Option<f64>,
    /// Fraction of row groups whose min equals max.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_point_equal_fraction: Option<f64>,
    /// Data pages, when indexes were loaded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_count: Option<u64>,
}

/// Rewrite a dump and measure each requested trial against a control write.
///
/// # Errors
/// Fails when a rewrite spec is unknown, a named column is missing, or the
/// writer cannot emit Parquet.
pub fn experiment(dump: &Dump, request: &ExperimentRequest) -> Result<Experiment, Error> {
    if dump.rows.is_empty() {
        return Err(Error("sample has no rows".into()));
    }
    let dump = flatten(dump);
    let parsed: Vec<TrialSpec> = request
        .trials
        .iter()
        .map(|spec| TrialSpec::parse(spec, &dump.columns))
        .collect::<Result<_, _>>()?;
    let control_spec = TrialSpec::control();
    let control_bytes = write_trial(&dump, &control_spec, request.aim)?;
    let control = measure_trial(
        "control",
        &control_spec,
        &control_bytes,
        request,
        None,
        dump.rows.len() as u64,
    )?;
    let mut trials = vec![control.clone()];
    for spec in parsed {
        let bytes = write_trial(&dump, &spec, request.aim)?;
        trials.push(measure_trial(
            &spec.source,
            &spec,
            &bytes,
            request,
            Some(&control),
            dump.rows.len() as u64,
        )?);
    }
    Ok(Experiment {
        num_rows: dump.rows.len() as u64,
        capabilities: capabilities(),
        aim: aim_name(request.aim).to_string(),
        trials,
    })
}

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
                "drop:COL".into(),
            ],
        },
    ]
}

fn aim_name(aim: Aim) -> &'static str {
    match aim {
        Aim::STORAGE => "storage",
        Aim::SKIPPING => "skipping",
        Aim::ALL => "all",
    }
}

#[derive(Debug, Clone)]
struct TrialSpec {
    source: String,
    rewrites: Vec<String>,
    layout: Layout,
    codec: Option<Compression>,
    dictionary: Option<bool>,
    dictionary_bytes: Option<usize>,
    encoding: Option<Encoding>,
    page_size: Option<usize>,
    row_group_rows: Option<usize>,
    casts: Vec<(String, CastTo)>,
    drops: Vec<String>,
}

#[derive(Debug, Clone)]
enum Layout {
    Keep,
    Sort(Vec<String>),
    ZOrder(Vec<String>),
    Hilbert(Vec<String>),
}

#[derive(Debug, Clone, Copy)]
enum CastTo {
    Int64,
    Double,
    String,
}

impl TrialSpec {
    fn control() -> Self {
        Self {
            source: "control".into(),
            rewrites: Vec::new(),
            layout: Layout::Keep,
            codec: None,
            dictionary: None,
            dictionary_bytes: None,
            encoding: None,
            page_size: None,
            row_group_rows: None,
            casts: Vec::new(),
            drops: Vec::new(),
        }
    }

    fn parse(source: &str, columns: &[String]) -> Result<Self, Error> {
        let mut spec = Self {
            source: source.to_string(),
            rewrites: Vec::new(),
            ..Self::control()
        };
        spec.source = source.to_string();
        for part in source.split(';') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            spec.rewrites.push(part.to_string());
            apply_rewrite(&mut spec, part, columns)?;
        }
        if spec.rewrites.is_empty() {
            return Err(Error("trial has no rewrites".into()));
        }
        Ok(spec)
    }
}

fn apply_rewrite(spec: &mut TrialSpec, part: &str, columns: &[String]) -> Result<(), Error> {
    let (kind, value) = part.split_once(':').ok_or_else(|| {
        Error(format!(
            "rewrite `{part}` must be kind:value (sort, zorder, hilbert, codec, dictionary, encoding, page-size, row-group-size, cast, drop)"
        ))
    })?;
    match kind {
        "sort" => spec.layout = Layout::Sort(column_list(value, columns)?),
        "zorder" => spec.layout = Layout::ZOrder(column_list(value, columns)?),
        "hilbert" => {
            let names = column_list(value, columns)?;
            if names.len() != 2 {
                return Err(Error(
                    "hilbert needs exactly two columns; use zorder for more".into(),
                ));
            }
            spec.layout = Layout::Hilbert(names);
        }
        "codec" => spec.codec = Some(parse_codec(value)?),
        "dictionary" => match value {
            "on" => spec.dictionary = Some(true),
            "off" => spec.dictionary = Some(false),
            bytes => {
                spec.dictionary = Some(true);
                spec.dictionary_bytes = Some(parse_usize(bytes, "dictionary")?);
            }
        },
        "encoding" => spec.encoding = Some(parse_encoding(value)?),
        "page-size" => spec.page_size = Some(parse_usize(value, "page-size")?),
        "row-group-size" => spec.row_group_rows = Some(parse_usize(value, "row-group-size")?),
        "cast" => spec.casts.push(parse_cast(value, columns)?),
        "drop" => {
            let name = require_column(value, columns)?;
            spec.drops.push(name);
        }
        other => {
            return Err(Error(format!(
                "unknown rewrite `{other}`; expected sort, zorder, hilbert, codec, dictionary, encoding, page-size, row-group-size, cast, drop"
            )));
        }
    }
    Ok(())
}

fn column_list(value: &str, columns: &[String]) -> Result<Vec<String>, Error> {
    let names: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| require_column(name, columns))
        .collect::<Result<_, _>>()?;
    if names.is_empty() {
        return Err(Error("rewrite needs at least one column".into()));
    }
    Ok(names)
}

fn require_column(name: &str, columns: &[String]) -> Result<String, Error> {
    columns
        .iter()
        .find(|column| column.as_str() == name)
        .cloned()
        .ok_or_else(|| Error(format!("column `{name}` is not in the sample")))
}

fn parse_usize(value: &str, name: &str) -> Result<usize, Error> {
    let parsed: usize = value
        .parse()
        .map_err(|_| Error(format!("{name} `{value}` is not a positive integer")))?;
    if parsed == 0 {
        return Err(Error(format!("{name} must be >= 1")));
    }
    Ok(parsed)
}

fn parse_codec(value: &str) -> Result<Compression, Error> {
    let (name, level) = match value.split_once('@') {
        Some((name, level)) => (name, Some(parse_usize(level, "codec level")? as i32)),
        None => (value, None),
    };
    match name {
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => {
            let level = parquet::basic::GzipLevel::try_new(level.unwrap_or(6) as u32)
                .map_err(|error| Error(error.to_string()))?;
            Ok(Compression::GZIP(level))
        }
        "lz4" => Ok(Compression::LZ4_RAW),
        "zstd" => {
            let level = parquet::basic::ZstdLevel::try_new(level.unwrap_or(3))
                .map_err(|error| Error(error.to_string()))?;
            Ok(Compression::ZSTD(level))
        }
        other => Err(Error(format!(
            "unknown codec `{other}`; expected uncompressed, snappy, gzip, lz4, zstd"
        ))),
    }
}

fn parse_encoding(value: &str) -> Result<Encoding, Error> {
    match value {
        "plain" => Ok(Encoding::PLAIN),
        "delta" => Ok(Encoding::DELTA_BINARY_PACKED),
        "rle" => Ok(Encoding::RLE),
        "delta_length" => Ok(Encoding::DELTA_LENGTH_BYTE_ARRAY),
        "delta_byte_array" => Ok(Encoding::DELTA_BYTE_ARRAY),
        "byte_stream_split" => Ok(Encoding::BYTE_STREAM_SPLIT),
        other => Err(Error(format!(
            "unknown encoding `{other}`; expected plain, delta, rle, delta_length, delta_byte_array, byte_stream_split"
        ))),
    }
}

fn parse_cast(value: &str, columns: &[String]) -> Result<(String, CastTo), Error> {
    let (column, to) = value.split_once(':').ok_or_else(|| {
        Error(format!(
            "cast `{value}` must be COL:int64, COL:double, or COL:string"
        ))
    })?;
    let column = require_column(column, columns)?;
    let to = match to {
        "int64" => CastTo::Int64,
        "double" => CastTo::Double,
        "string" => CastTo::String,
        other => {
            return Err(Error(format!(
                "unknown cast `{other}`; expected int64, double, or string"
            )));
        }
    };
    Ok((column, to))
}

fn write_trial(dump: &Dump, spec: &TrialSpec, aim: Aim) -> Result<Vec<u8>, Error> {
    let mut dump = apply_model(dump, spec)?;
    apply_layout(&mut dump, &spec.layout)?;
    let kinds = column_kinds(&dump);
    let schema = schema_from_kinds(&dump.columns, &kinds)?;
    let row_group_rows = row_group_rows(spec, dump.rows.len(), aim);
    let props = writer_properties(spec, &dump.columns, &spec.layout)?;
    let mut out = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut out, schema, Arc::new(props))
        .map_err(|error| Error(error.to_string()))?;
    for chunk in dump.rows.chunks(row_group_rows) {
        let mut group = writer
            .next_row_group()
            .map_err(|error| Error(error.to_string()))?;
        for (index, kind) in kinds.iter().enumerate() {
            let mut column = group
                .next_column()
                .map_err(|error| Error(error.to_string()))?
                .ok_or_else(|| Error("writer ran out of columns".into()))?;
            let values: Vec<&Value> = chunk
                .iter()
                .map(|row| row.get(index).unwrap_or(&Value::Null))
                .collect();
            write_column(&mut column, *kind, &values)?;
            column.close().map_err(|error| Error(error.to_string()))?;
        }
        group.close().map_err(|error| Error(error.to_string()))?;
    }
    writer.close().map_err(|error| Error(error.to_string()))?;
    Ok(out)
}

fn apply_model(dump: &Dump, spec: &TrialSpec) -> Result<Dump, Error> {
    let keep: Vec<usize> = dump
        .columns
        .iter()
        .enumerate()
        .filter(|(_, name)| !spec.drops.iter().any(|drop| drop == *name))
        .map(|(index, _)| index)
        .collect();
    if keep.is_empty() {
        return Err(Error("drop removed every column".into()));
    }
    let columns: Vec<String> = keep
        .iter()
        .map(|&index| dump.columns[index].clone())
        .collect();
    let mut rows: Vec<Vec<Value>> = dump
        .rows
        .iter()
        .map(|row| {
            keep.iter()
                .map(|&index| row.get(index).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    for (name, to) in &spec.casts {
        let index = columns
            .iter()
            .position(|column| column == name)
            .ok_or_else(|| Error(format!("cast column `{name}` was dropped")))?;
        for row in &mut rows {
            row[index] = cast_value(&row[index], *to, name)?;
        }
    }
    Ok(Dump { columns, rows })
}

fn cast_value(value: &Value, to: CastTo, name: &str) -> Result<Value, Error> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match to {
        CastTo::Int64 => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|n| i64::try_from(n).ok()))
            .or_else(|| {
                value
                    .as_f64()
                    .and_then(|n| (n.fract() == 0.0).then_some(n as i64))
            })
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            .map(Value::from)
            .ok_or_else(|| Error(format!("cannot cast `{name}` to int64"))),
        CastTo::Double => value
            .as_f64()
            .or_else(|| value.as_i64().map(|n| n as f64))
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or_else(|| Error(format!("cannot cast `{name}` to double"))),
        CastTo::String => Ok(Value::String(match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        })),
    }
}

fn apply_layout(dump: &mut Dump, layout: &Layout) -> Result<(), Error> {
    match layout {
        Layout::Keep => Ok(()),
        Layout::Sort(names) => {
            let indexes = indexes_of(dump, names)?;
            dump.rows
                .sort_by(|left, right| compare_keys(left, right, &indexes));
            Ok(())
        }
        Layout::ZOrder(names) => {
            let keys = space_keys(dump, names, Space::ZOrder)?;
            order_by(dump, &keys);
            Ok(())
        }
        Layout::Hilbert(names) => {
            let keys = space_keys(dump, names, Space::Hilbert)?;
            order_by(dump, &keys);
            Ok(())
        }
    }
}

fn indexes_of(dump: &Dump, names: &[String]) -> Result<Vec<usize>, Error> {
    names
        .iter()
        .map(|name| {
            dump.columns
                .iter()
                .position(|column| column == name)
                .ok_or_else(|| Error(format!("column `{name}` is not in the sample")))
        })
        .collect()
}

fn compare_keys(left: &[Value], right: &[Value], indexes: &[usize]) -> std::cmp::Ordering {
    for &index in indexes {
        let order = compare_values(
            left.get(index).unwrap_or(&Value::Null),
            right.get(index).unwrap_or(&Value::Null),
        );
        if order != std::cmp::Ordering::Equal {
            return order;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (as_number(left), as_number(right)) {
        (None, None) if left.is_null() && right.is_null() => std::cmp::Ordering::Equal,
        (None, _) if left.is_null() => std::cmp::Ordering::Greater,
        (_, None) if right.is_null() => std::cmp::Ordering::Less,
        (Some(left), Some(right)) => left
            .partial_cmp(&right)
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => value_key(left).cmp(&value_key(right)),
    }
}

fn as_number(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_i64().map(|n| n as f64))
}

fn order_by(dump: &mut Dump, keys: &[u128]) {
    let mut order: Vec<usize> = (0..dump.rows.len()).collect();
    order.sort_by_key(|&index| keys[index]);
    dump.rows = order
        .into_iter()
        .map(|index| dump.rows[index].clone())
        .collect();
}

#[derive(Clone, Copy)]
enum Space {
    ZOrder,
    Hilbert,
}

fn space_keys(dump: &Dump, names: &[String], space: Space) -> Result<Vec<u128>, Error> {
    let indexes = indexes_of(dump, names)?;
    let ranks: Vec<Vec<u32>> = indexes.iter().map(|&index| ranks_of(dump, index)).collect();
    Ok((0..dump.rows.len())
        .map(|row| match space {
            Space::ZOrder => zorder_key(ranks.iter().map(|column| column[row])),
            Space::Hilbert => hilbert_key(ranks[0][row], ranks[1][row]),
        })
        .collect())
}

fn ranks_of(dump: &Dump, index: usize) -> Vec<u32> {
    let mut unique: Vec<Value> = dump
        .rows
        .iter()
        .map(|row| row.get(index).cloned().unwrap_or(Value::Null))
        .collect();
    unique.sort_by(compare_values);
    unique.dedup();
    let map: BTreeMap<String, u32> = unique
        .into_iter()
        .enumerate()
        .map(|(rank, value)| (value_key(&value), rank as u32))
        .collect();
    dump.rows
        .iter()
        .map(|row| {
            *map.get(&value_key(row.get(index).unwrap_or(&Value::Null)))
                .unwrap_or(&0)
        })
        .collect()
}

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

#[derive(Clone, Copy)]
enum Kind {
    Bool,
    Int,
    Float,
    String,
}

fn column_kinds(dump: &Dump) -> Vec<Kind> {
    dump.columns
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let mut saw_bool = false;
            let mut saw_int = false;
            let mut saw_float = false;
            let mut saw_string = false;
            for row in &dump.rows {
                match row.get(index).unwrap_or(&Value::Null) {
                    Value::Null => {}
                    Value::Bool(_) => saw_bool = true,
                    Value::Number(number) if number.is_i64() || number.is_u64() => saw_int = true,
                    Value::Number(_) => saw_float = true,
                    _ => saw_string = true,
                }
            }
            if saw_string {
                Kind::String
            } else if saw_float {
                Kind::Float
            } else if saw_int {
                Kind::Int
            } else if saw_bool {
                Kind::Bool
            } else {
                Kind::String
            }
        })
        .collect()
}

fn schema_from_kinds(columns: &[String], kinds: &[Kind]) -> Result<Arc<Type>, Error> {
    let fields = columns
        .iter()
        .zip(kinds.iter())
        .map(|(name, kind)| {
            let physical = match kind {
                Kind::Bool => PhysicalType::BOOLEAN,
                Kind::Int => PhysicalType::INT64,
                Kind::Float => PhysicalType::DOUBLE,
                Kind::String => PhysicalType::BYTE_ARRAY,
            };
            let mut builder =
                Type::primitive_type_builder(name, physical).with_repetition(Repetition::OPTIONAL);
            if matches!(kind, Kind::String) {
                builder = builder
                    .with_converted_type(ConvertedType::UTF8)
                    .with_logical_type(Some(LogicalType::String));
            }
            builder
                .build()
                .map(Arc::new)
                .map_err(|error| Error(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Type::group_type_builder("schema")
        .with_fields(fields)
        .build()
        .map(Arc::new)
        .map_err(|error| Error(error.to_string()))
}

fn row_group_rows(spec: &TrialSpec, rows: usize, aim: Aim) -> usize {
    if let Some(size) = spec.row_group_rows {
        return size.max(1);
    }
    if aim.skipping() {
        return (rows / 8).max(16).min(rows.max(1));
    }
    rows.max(1)
}

fn writer_properties(
    spec: &TrialSpec,
    columns: &[String],
    layout: &Layout,
) -> Result<WriterProperties, Error> {
    let mut builder = WriterProperties::builder()
        .set_compression(spec.codec.unwrap_or(Compression::ZSTD(Default::default())));
    if let Some(dictionary) = spec.dictionary {
        builder = builder.set_dictionary_enabled(dictionary);
    }
    if let Some(bytes) = spec.dictionary_bytes {
        builder = builder.set_dictionary_page_size_limit(bytes);
    }
    if let Some(encoding) = spec.encoding {
        builder = builder.set_encoding(encoding);
    }
    if let Some(page_size) = spec.page_size {
        builder = builder.set_data_page_size_limit(page_size);
    }
    if let Layout::Sort(names) = layout {
        let sorting = names
            .iter()
            .map(|name| {
                let column_idx = columns
                    .iter()
                    .position(|column| column == name)
                    .ok_or_else(|| Error(format!("sort column `{name}` was dropped")))?;
                Ok(SortingColumn {
                    column_idx: i32::try_from(column_idx).unwrap_or(0),
                    descending: false,
                    nulls_first: false,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        builder = builder.set_sorting_columns(Some(sorting));
    }
    Ok(builder.build())
}

fn write_column(
    column: &mut parquet::file::writer::SerializedColumnWriter<'_>,
    kind: Kind,
    values: &[&Value],
) -> Result<(), Error> {
    match kind {
        Kind::Bool => {
            let (data, def) = collect_bool(values);
            column
                .typed::<BoolType>()
                .write_batch(&data, Some(&def), None)
                .map_err(|error| Error(error.to_string()))?;
        }
        Kind::Int => {
            let (data, def) = collect_i64(values);
            column
                .typed::<Int64Type>()
                .write_batch(&data, Some(&def), None)
                .map_err(|error| Error(error.to_string()))?;
        }
        Kind::Float => {
            let (data, def) = collect_f64(values);
            column
                .typed::<DoubleType>()
                .write_batch(&data, Some(&def), None)
                .map_err(|error| Error(error.to_string()))?;
        }
        Kind::String => {
            let (data, def) = collect_bytes(values);
            column
                .typed::<ByteArrayType>()
                .write_batch(&data, Some(&def), None)
                .map_err(|error| Error(error.to_string()))?;
        }
    }
    Ok(())
}

fn collect_bool(values: &[&Value]) -> (Vec<bool>, Vec<i16>) {
    let mut data = Vec::new();
    let mut def = Vec::new();
    for value in values {
        match value.as_bool() {
            Some(flag) => {
                data.push(flag);
                def.push(1);
            }
            None => def.push(0),
        }
    }
    (data, def)
}

fn collect_i64(values: &[&Value]) -> (Vec<i64>, Vec<i16>) {
    let mut data = Vec::new();
    let mut def = Vec::new();
    for value in values {
        match value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|n| i64::try_from(n).ok()))
        {
            Some(number) => {
                data.push(number);
                def.push(1);
            }
            None => def.push(0),
        }
    }
    (data, def)
}

fn collect_f64(values: &[&Value]) -> (Vec<f64>, Vec<i16>) {
    let mut data = Vec::new();
    let mut def = Vec::new();
    for value in values {
        match value.as_f64().or_else(|| value.as_i64().map(|n| n as f64)) {
            Some(number) => {
                data.push(number);
                def.push(1);
            }
            None => def.push(0),
        }
    }
    (data, def)
}

fn collect_bytes(values: &[&Value]) -> (Vec<ByteArray>, Vec<i16>) {
    let mut data = Vec::new();
    let mut def = Vec::new();
    for value in values {
        if value.is_null() {
            def.push(0);
            continue;
        }
        let text = match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        data.push(ByteArray::from(text.as_bytes()));
        def.push(1);
    }
    (data, def)
}

fn measure_trial(
    name: &str,
    spec: &TrialSpec,
    bytes: &[u8],
    request: &ExperimentRequest,
    control: Option<&Trial>,
    num_rows: u64,
) -> Result<Trial, Error> {
    let indexes = request.indexes || request.aim.skipping();
    let mass = parquet_helpers::read_buffer_masses(bytes, indexes)?;
    let file_bytes = Some(bytes.len() as u64);
    let bytes_per_row = file_bytes.map(|size| size as f64 / num_rows.max(1) as f64);
    let file_bytes_ratio = match (file_bytes, control.and_then(|trial| trial.file_bytes)) {
        (Some(size), Some(base)) if base > 0 => Some(size as f64 / base as f64),
        _ => None,
    };
    let columns = column_trials(&mass, control, num_rows, request.aim);
    Ok(Trial {
        name: name.to_string(),
        rewrites: spec.rewrites.clone(),
        file_bytes: request.aim.storage().then_some(file_bytes).flatten(),
        bytes_per_row: request.aim.storage().then_some(bytes_per_row).flatten(),
        file_bytes_ratio: request.aim.storage().then_some(file_bytes_ratio).flatten(),
        row_group_count: request
            .aim
            .skipping()
            .then_some(mass.row_group_count as u64),
        columns,
    })
}

fn column_trials(
    mass: &FileMass,
    control: Option<&Trial>,
    num_rows: u64,
    aim: Aim,
) -> Vec<ColumnTrial> {
    let mut grouped: BTreeMap<String, Vec<&parquet_helpers::ColumnMass>> = BTreeMap::new();
    for column in &mass.columns {
        grouped.entry(column.path.clone()).or_default().push(column);
    }
    grouped
        .into_iter()
        .map(|(name, chunks)| {
            let compressed: u64 = chunks.iter().map(|chunk| chunk.bytes).sum();
            let pages: Option<u64> = chunks
                .iter()
                .map(|chunk| chunk.page_count)
                .try_fold(0, |sum, page| Some(sum + page?));
            let control_column =
                control.and_then(|trial| trial.columns.iter().find(|column| column.column == name));
            ColumnTrial {
                column: name,
                compressed_bytes: aim.storage().then_some(compressed),
                bytes_per_row: aim
                    .storage()
                    .then_some(compressed as f64 / num_rows.max(1) as f64),
                compressed_bytes_ratio: match (
                    aim.storage(),
                    control_column.and_then(|column| column.compressed_bytes),
                ) {
                    (true, Some(base)) if base > 0 => Some(compressed as f64 / base as f64),
                    _ => None,
                },
                skip_span_ratio: aim.skipping().then(|| skip_span(&chunks)).flatten(),
                skip_point_equal_fraction: aim.skipping().then(|| skip_equal(&chunks)),
                page_count: if aim.skipping() { pages } else { None },
            }
        })
        .collect()
}

fn skip_span(chunks: &[&parquet_helpers::ColumnMass]) -> Option<f64> {
    let mins: Vec<f64> = chunks
        .iter()
        .filter_map(|chunk| parse_num(chunk.min_value.as_deref()))
        .collect();
    let maxs: Vec<f64> = chunks
        .iter()
        .filter_map(|chunk| parse_num(chunk.max_value.as_deref()))
        .collect();
    if mins.len() != chunks.len() || maxs.len() != chunks.len() || mins.is_empty() {
        return None;
    }
    let global_min = mins.iter().copied().fold(f64::INFINITY, f64::min);
    let global_max = maxs.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let global = global_max - global_min;
    if global == 0.0 {
        return Some(0.0);
    }
    let mean = mins
        .iter()
        .zip(maxs.iter())
        .map(|(min, max)| (max - min) / global)
        .sum::<f64>()
        / mins.len() as f64;
    Some(mean)
}

fn skip_equal(chunks: &[&parquet_helpers::ColumnMass]) -> f64 {
    if chunks.is_empty() {
        return 0.0;
    }
    let hits = chunks
        .iter()
        .filter(|chunk| chunk.min_value.is_some() && chunk.min_value == chunk.max_value)
        .count();
    hits as f64 / chunks.len() as f64
}

fn parse_num(value: Option<&str>) -> Option<f64> {
    value?.parse().ok()
}

fn flatten(dump: &Dump) -> Dump {
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

fn value_key(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
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
    fn control_is_always_measured() {
        let sample = dump(&["n"], (0..32).map(|n| vec![json!(n)]).collect());
        let result = experiment(&sample, &ExperimentRequest::default()).unwrap();
        assert_eq!(result.trials.len(), 1);
        assert_eq!(result.trials[0].name, "control");
        assert!(result.trials[0].file_bytes.unwrap() > 0);
    }

    #[test]
    fn sort_improves_skip_span_on_a_shuffled_key() {
        let rows: Vec<Vec<Value>> = (0..64)
            .map(|n| vec![json!((n * 7) % 64), json!(n * 10)])
            .collect();
        let sample = dump(&["id", "payload"], rows);
        let result = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["sort:id".into()],
                aim: Aim::SKIPPING,
                indexes: false,
            },
        )
        .unwrap();
        let control = result
            .trials
            .iter()
            .find(|trial| trial.name == "control")
            .unwrap()
            .columns
            .iter()
            .find(|column| column.column == "id")
            .unwrap()
            .skip_span_ratio
            .unwrap();
        let sorted = result
            .trials
            .iter()
            .find(|trial| trial.name == "sort:id")
            .unwrap()
            .columns
            .iter()
            .find(|column| column.column == "id")
            .unwrap()
            .skip_span_ratio
            .unwrap();
        assert!(
            sorted < control,
            "sorted span {sorted} should be tighter than control {control}"
        );
    }

    #[test]
    fn uncompressed_is_larger_than_zstd_on_repeated_text() {
        let rows = (0..256)
            .map(|_| vec![json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")])
            .collect();
        let sample = dump(&["text"], rows);
        let result = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["codec:uncompressed".into()],
                aim: Aim::STORAGE,
                indexes: false,
            },
        )
        .unwrap();
        let control = result.trials[0].file_bytes.unwrap();
        let raw = result
            .trials
            .iter()
            .find(|trial| trial.name == "codec:uncompressed")
            .unwrap()
            .file_bytes
            .unwrap();
        assert!(
            raw > control,
            "uncompressed {raw} should exceed zstd {control}"
        );
    }

    #[test]
    fn unknown_rewrite_is_an_error() {
        let sample = dump(&["a"], vec![vec![json!(1)]]);
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
    fn cast_and_drop_are_requestable() {
        let sample = dump(
            &["id", "extra"],
            vec![
                vec![json!("10"), json!(true)],
                vec![json!("11"), json!(false)],
            ],
        );
        let result = experiment(
            &sample,
            &ExperimentRequest {
                trials: vec!["cast:id:int64;drop:extra".into()],
                aim: Aim::STORAGE,
                indexes: false,
            },
        )
        .unwrap();
        let trial = result
            .trials
            .iter()
            .find(|trial| {
                trial
                    .rewrites
                    .iter()
                    .any(|rewrite| rewrite.contains("cast"))
            })
            .unwrap();
        assert!(trial.columns.iter().any(|column| column.column == "id"));
        assert!(trial.columns.iter().all(|column| column.column != "extra"));
    }
}
