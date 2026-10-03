//! The bundled `PageParser` implementation, backed by parquet-rs.
//!
//! This module is private (`lib.rs` doesn't `pub mod` it); the only thing the
//! crate exposes is [`super::api::default_parser`]. Tool code never
//! imports `parquet` directly.

use std::path::Path;
use std::sync::Arc;

use parquet::basic::{
    Compression, Encoding, GzipLevel, Repetition, Type as PhysicalType, ZstdLevel,
};
use parquet::column::page::{Page as ParquetPage, PageReader};
use parquet::data_type::{AsBytes, BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::metadata::{
    PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, SortingColumn,
};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::record::Field;
use parquet::schema::types::{ColumnPath, Type};

use super::api::{
    ColumnChunk, ColumnMass, Error, FileMass, FileMetadata, Kind, MetadataEntry, MetadataParser,
    Page, PageParser, ParquetFile, RowGroupMetadata, Sample, TypedColumn, TypedSample, Value,
    WriteOptions,
};

/// The parquet-rs-backed page parser.
pub struct ParquetRsParser;

impl PageParser for ParquetRsParser {
    /// Read a parquet file from an in-memory byte slice and extract its pages.
    ///
    /// No disk I/O: `bytes` is wrapped as an in-memory `ChunkReader` and every
    /// page payload is copied out into the returned [`ParquetFile`].
    fn parse_pages(&self, bytes: &[u8]) -> Result<ParquetFile, Error> {
        let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(bytes))?;
        let mut file = ParquetFile { chunks: Vec::new() };
        for rg in 0..reader.num_row_groups() {
            let row_group = reader.get_row_group(rg)?;
            for col in 0..row_group.num_columns() {
                let meta = row_group.metadata().column(col);
                let column = meta.column_path().string();
                let compression = meta.compression();
                if compression != Compression::UNCOMPRESSED {
                    return Err(Error(format!(
                        "only NONE (uncompressed) parquet is supported; column {column} is {compression}"
                    )));
                }
                let pages = collect_pages(row_group.get_column_page_reader(col)?)?;
                file.chunks.push(ColumnChunk { column, pages });
            }
        }
        Ok(file)
    }
}

impl MetadataParser for ParquetRsParser {
    /// Read a parquet file's per-column byte masses from its footer metadata.
    ///
    /// Only the footer is read: no page is decoded, so compressed columns are
    /// fine, and large files aren't loaded into memory.
    fn read_masses(&self, path: &Path) -> Result<FileMass, Error> {
        read_file_masses(path, false)
    }
}

/// Decode byte masses from a complete Parquet footer without reading pages.
pub(crate) fn read_footer_masses(footer: &[u8]) -> Result<FileMass, Error> {
    let metadata =
        ParquetMetaDataReader::new().parse_and_finish(&bytes::Bytes::copy_from_slice(footer))?;
    create_masses(&metadata)
}

/// Lowest ColumnIndex/OffsetIndex offset in a footer, when one exists.
pub(crate) fn page_index_start(footer: &[u8]) -> Result<Option<u64>, Error> {
    let metadata =
        ParquetMetaDataReader::new().parse_and_finish(&bytes::Bytes::copy_from_slice(footer))?;
    Ok(index_start(&metadata))
}

/// Decode footer + page indexes from a file suffix (`tail` ends at `file_size`).
pub(crate) fn read_tail_masses(tail: &[u8], file_size: u64) -> Result<FileMass, Error> {
    let mut reader = ParquetMetaDataReader::new().with_page_index_policy(PageIndexPolicy::Optional);
    reader.try_parse_sized(&bytes::Bytes::copy_from_slice(tail), file_size)?;
    create_masses(&reader.finish()?)
}

/// Decode up to `max_rows` leading rows of a local file into stringified cells.
pub(crate) fn read_sample(path: &Path, max_rows: Option<usize>) -> Result<Sample, Error> {
    let file = std::fs::File::open(path).map_err(|error| Error(error.to_string()))?;
    let reader = SerializedFileReader::new(file)?;
    let mut sample = Sample::default();
    for row in reader.get_row_iter(None)? {
        if max_rows.is_some_and(|max| sample.rows.len() >= max) {
            break;
        }
        let row = row?;
        if sample.columns.is_empty() {
            sample.columns = row
                .get_column_iter()
                .map(|(name, _)| name.clone())
                .collect();
        }
        sample.rows.push(
            row.get_column_iter()
                .map(|(_, field)| field_text(field))
                .collect(),
        );
    }
    Ok(sample)
}

/// The display text of one decoded value; nulls have none.
fn field_text(field: &Field) -> Option<String> {
    match field {
        Field::Null => None,
        Field::Str(value) => Some(value.clone()),
        Field::Bytes(value) => Some(String::from_utf8_lossy(value.data()).into_owned()),
        other => Some(other.to_string()),
    }
}

/// Decode up to `max_rows` leading rows of a local file into typed cells.
pub(crate) fn read_typed_sample(
    path: &Path,
    max_rows: Option<usize>,
) -> Result<TypedSample, Error> {
    let file = std::fs::File::open(path).map_err(|error| Error(error.to_string()))?;
    let reader = SerializedFileReader::new(file)?;
    let mut sample = TypedSample::default();
    let mut kinds: Vec<Option<Kind>> = Vec::new();
    for row in reader.get_row_iter(None)? {
        if max_rows.is_some_and(|max| sample.rows.len() >= max) {
            break;
        }
        let row = row?;
        if sample.columns.is_empty() {
            sample.columns = row
                .get_column_iter()
                .map(|(name, field)| TypedColumn {
                    name: name.clone(),
                    kind: field_kind(field).unwrap_or(Kind::Bytes),
                })
                .collect();
            kinds = vec![None; sample.columns.len()];
        }
        for (index, (_, field)) in row.get_column_iter().enumerate() {
            if kinds.get(index).is_some_and(Option::is_none) {
                kinds[index] = field_kind(field);
            }
        }
        sample.rows.push(
            row.get_column_iter()
                .map(|(_, field)| field_value(field))
                .collect(),
        );
    }
    for (column, kind) in sample.columns.iter_mut().zip(kinds.iter()) {
        column.kind = kind.unwrap_or(Kind::Bytes);
    }
    Ok(sample)
}

/// The typed kind of a decoded value; nulls have none.
fn field_kind(field: &Field) -> Option<Kind> {
    match field {
        Field::Null => None,
        Field::Bool(_) => Some(Kind::Boolean),
        Field::Byte(_)
        | Field::Short(_)
        | Field::Int(_)
        | Field::Long(_)
        | Field::UByte(_)
        | Field::UShort(_)
        | Field::UInt(_)
        | Field::ULong(_)
        | Field::Date(_)
        | Field::TimeMillis(_)
        | Field::TimeMicros(_)
        | Field::TimestampMillis(_)
        | Field::TimestampMicros(_) => Some(Kind::Integer),
        Field::Float16(_) | Field::Float(_) | Field::Double(_) | Field::Decimal(_) => {
            Some(Kind::Number)
        }
        Field::Str(_)
        | Field::Bytes(_)
        | Field::Group(_)
        | Field::ListInternal(_)
        | Field::MapInternal(_) => Some(Kind::Bytes),
    }
}

/// The typed cell of a decoded value.
fn field_value(field: &Field) -> Value {
    match field {
        Field::Null => Value::Null,
        Field::Bool(value) => Value::Boolean(*value),
        Field::Byte(value) => Value::Integer(i64::from(*value)),
        Field::Short(value) => Value::Integer(i64::from(*value)),
        Field::Int(value) => Value::Integer(i64::from(*value)),
        Field::Long(value) => Value::Integer(*value),
        Field::UByte(value) => Value::Integer(i64::from(*value)),
        Field::UShort(value) => Value::Integer(i64::from(*value)),
        Field::UInt(value) => Value::Integer(i64::from(*value)),
        Field::ULong(value) => Value::Integer(i64::try_from(*value).unwrap_or(i64::MAX)),
        Field::Date(value) => Value::Integer(i64::from(*value)),
        Field::TimeMillis(value) => Value::Integer(i64::from(*value)),
        Field::TimeMicros(value) => Value::Integer(*value),
        Field::TimestampMillis(value) => Value::Integer(*value),
        Field::TimestampMicros(value) => Value::Integer(*value),
        Field::Float16(value) => Value::Number(value.to_f64()),
        Field::Float(value) => Value::Number(f64::from(*value)),
        Field::Double(value) => Value::Number(*value),
        Field::Decimal(_) => Value::Number(field.to_string().parse().unwrap_or(0.0)),
        Field::Str(value) => Value::Bytes(value.clone().into_bytes()),
        Field::Bytes(value) => Value::Bytes(value.data().to_vec()),
        Field::Group(_) | Field::ListInternal(_) | Field::MapInternal(_) => {
            Value::Bytes(field.to_string().into_bytes())
        }
    }
}

/// Write typed columns and rows to an in-memory Parquet file.
pub(crate) fn write_parquet(
    columns: &[TypedColumn],
    rows: &[Vec<Value>],
    options: &WriteOptions,
) -> Result<Vec<u8>, Error> {
    if columns.is_empty() {
        return Err(Error("no columns to write".into()));
    }
    let schema = schema_from_columns(columns)?;
    let mut builder = WriterProperties::builder()
        .set_compression(parse_compression(&options.compression, options.level)?)
        .set_dictionary_enabled(options.dictionary);
    if let Some(bytes) = options.dictionary_bytes {
        builder = builder.set_dictionary_page_size_limit(bytes);
    }
    if let Some(encoding) = &options.encoding {
        let mut applied = 0usize;
        for column in columns {
            if let Some(encoding) = compatible_encoding(encoding, column.kind)? {
                builder =
                    builder.set_column_encoding(ColumnPath::from(column.name.clone()), encoding);
                applied += 1;
            }
        }
        if applied == 0 {
            return Err(Error(format!(
                "encoding `{encoding}` applies to no column in the sample"
            )));
        }
    }
    if let Some(page_size) = options.page_size {
        builder = builder.set_data_page_size_limit(page_size);
    }
    if !options.sorting_columns.is_empty() {
        let sorting = options
            .sorting_columns
            .iter()
            .map(|name| {
                let index = columns
                    .iter()
                    .position(|column| &column.name == name)
                    .ok_or_else(|| {
                        Error(format!("sorting column `{name}` is not in the sample"))
                    })?;
                Ok(SortingColumn {
                    column_idx: i32::try_from(index).unwrap_or(0),
                    descending: false,
                    nulls_first: false,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        builder = builder.set_sorting_columns(Some(sorting));
    }
    let properties = builder.build();
    let group_rows = if options.row_group_size == 0 {
        rows.len().max(1)
    } else {
        options.row_group_size
    };
    let mut out = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut out, schema, Arc::new(properties))?;
    for chunk in rows.chunks(group_rows) {
        let mut group = writer.next_row_group()?;
        for (index, column) in columns.iter().enumerate() {
            let mut writer = group
                .next_column()?
                .ok_or_else(|| Error("writer ran out of columns".into()))?;
            let values: Vec<&Value> = chunk
                .iter()
                .map(|row| row.get(index).unwrap_or(&Value::Null))
                .collect();
            write_column(&mut writer, column.kind, &values)?;
            writer.close()?;
        }
        group.close()?;
    }
    writer.close()?;
    Ok(out)
}

// aipnaming: allow(aip-136/method-prepositions)
/// Build a Parquet schema from typed columns (all optional).
fn schema_from_columns(columns: &[TypedColumn]) -> Result<Arc<Type>, Error> {
    let fields = columns
        .iter()
        .map(|column| {
            let physical = match column.kind {
                Kind::Boolean => PhysicalType::BOOLEAN,
                Kind::Integer => PhysicalType::INT64,
                Kind::Number => PhysicalType::DOUBLE,
                Kind::Bytes => PhysicalType::BYTE_ARRAY,
            };
            Type::primitive_type_builder(&column.name, physical)
                .with_repetition(Repetition::OPTIONAL)
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

/// Parse a codec name and optional level into a Parquet `Compression`.
fn parse_compression(name: &str, level: Option<i32>) -> Result<Compression, Error> {
    match name {
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => {
            let level = u32::try_from(level.unwrap_or(6))
                .map_err(|_| Error("gzip level must be non-negative".into()))?;
            GzipLevel::try_new(level)
                .map(Compression::GZIP)
                .map_err(|error| Error(error.to_string()))
        }
        "lz4" => Ok(Compression::LZ4_RAW),
        "zstd" => ZstdLevel::try_new(level.unwrap_or(3))
            .map(Compression::ZSTD)
            .map_err(|error| Error(error.to_string())),
        other => Err(Error(format!(
            "unknown codec `{other}`; expected uncompressed, snappy, gzip, lz4, zstd"
        ))),
    }
}

/// The encoding for a column, or `None` when the name does not apply to its
/// kind. Parquet encodings are type-specific, so an incompatible pairing is
/// skipped rather than applied globally (which would panic in parquet-rs).
fn compatible_encoding(name: &str, kind: Kind) -> Result<Option<Encoding>, Error> {
    let encoding = match name {
        "plain" => Some(Encoding::PLAIN),
        "delta" if kind == Kind::Integer => Some(Encoding::DELTA_BINARY_PACKED),
        "rle" if kind == Kind::Boolean => Some(Encoding::RLE),
        "delta_length" if kind == Kind::Bytes => Some(Encoding::DELTA_LENGTH_BYTE_ARRAY),
        "delta_byte_array" if kind == Kind::Bytes => Some(Encoding::DELTA_BYTE_ARRAY),
        "byte_stream_split" if kind == Kind::Number => Some(Encoding::BYTE_STREAM_SPLIT),
        "delta" | "rle" | "delta_length" | "delta_byte_array" | "byte_stream_split" => None,
        other => {
            return Err(Error(format!(
                "unknown encoding `{other}`; expected plain, delta, rle, delta_length, delta_byte_array, byte_stream_split"
            )));
        }
    };
    Ok(encoding)
}

/// Write one column's typed values, marking nulls with definition levels.
fn write_column(
    writer: &mut SerializedColumnWriter<'_>,
    kind: Kind,
    values: &[&Value],
) -> Result<(), Error> {
    match kind {
        Kind::Boolean => {
            let (data, def) = collect_bool(values);
            writer
                .typed::<BoolType>()
                .write_batch(&data, Some(&def), None)?;
        }
        Kind::Integer => {
            let (data, def) = collect_i64(values);
            writer
                .typed::<Int64Type>()
                .write_batch(&data, Some(&def), None)?;
        }
        Kind::Number => {
            let (data, def) = collect_f64(values);
            writer
                .typed::<DoubleType>()
                .write_batch(&data, Some(&def), None)?;
        }
        Kind::Bytes => {
            let (data, def) = collect_bytes(values);
            writer
                .typed::<ByteArrayType>()
                .write_batch(&data, Some(&def), None)?;
        }
    }
    Ok(())
}

fn collect_bool(values: &[&Value]) -> (Vec<bool>, Vec<i16>) {
    let mut data = Vec::new();
    let mut def = Vec::new();
    for value in values {
        match value {
            Value::Boolean(flag) => {
                data.push(*flag);
                def.push(1);
            }
            _ => def.push(0),
        }
    }
    (data, def)
}

fn collect_i64(values: &[&Value]) -> (Vec<i64>, Vec<i16>) {
    let mut data = Vec::new();
    let mut def = Vec::new();
    for value in values {
        match value {
            Value::Integer(number) => {
                data.push(*number);
                def.push(1);
            }
            Value::Number(number) if number.fract() == 0.0 => {
                data.push(*number as i64);
                def.push(1);
            }
            _ => def.push(0),
        }
    }
    (data, def)
}

fn collect_f64(values: &[&Value]) -> (Vec<f64>, Vec<i16>) {
    let mut data = Vec::new();
    let mut def = Vec::new();
    for value in values {
        match value {
            Value::Number(number) => {
                data.push(*number);
                def.push(1);
            }
            Value::Integer(number) => {
                data.push(*number as f64);
                def.push(1);
            }
            _ => def.push(0),
        }
    }
    (data, def)
}

fn collect_bytes(values: &[&Value]) -> (Vec<ByteArray>, Vec<i16>) {
    let mut data = Vec::new();
    let mut def = Vec::new();
    for value in values {
        match value {
            Value::Bytes(bytes) => {
                data.push(ByteArray::from(bytes.as_slice()));
                def.push(1);
            }
            Value::Null => def.push(0),
            other => {
                data.push(ByteArray::from(value_text(other).as_bytes()));
                def.push(1);
            }
        }
    }
    (data, def)
}

/// The display text of a typed value.
fn value_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Boolean(flag) => flag.to_string(),
        Value::Integer(number) => number.to_string(),
        Value::Number(number) => number.to_string(),
        Value::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

/// Read byte masses from an in-memory Parquet file.
pub(crate) fn read_buffer_masses(bytes: &[u8], indexes: bool) -> Result<FileMass, Error> {
    let policy = if indexes {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(policy)
        .parse_and_finish(&bytes::Bytes::copy_from_slice(bytes))?;
    create_masses(&metadata)
}

fn index_start(metadata: &ParquetMetaData) -> Option<u64> {
    metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns())
        .filter_map(|column| {
            let column_index = column
                .column_index_offset()
                .and_then(|offset| u64::try_from(offset).ok())
                .filter(|offset| *offset > 0);
            let offset_index = column
                .offset_index_offset()
                .and_then(|offset| u64::try_from(offset).ok())
                .filter(|offset| *offset > 0);
            match (column_index, offset_index) {
                (Some(left), Some(right)) => Some(left.min(right)),
                (Some(offset), None) | (None, Some(offset)) => Some(offset),
                (None, None) => None,
            }
        })
        .min()
}

/// Read byte masses from a local file. `indexes` loads ColumnIndex/OffsetIndex.
pub(crate) fn read_file_masses(path: &Path, indexes: bool) -> Result<FileMass, Error> {
    let file = std::fs::File::open(path).map_err(|error| Error(error.to_string()))?;
    let policy = if indexes {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(policy)
        .parse_and_finish(&file)?;
    create_masses(&metadata)
}

fn create_masses(metadata: &ParquetMetaData) -> Result<FileMass, Error> {
    let mut row_count = 0i64;
    let mut columns = Vec::new();
    for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
        let row_group_rows = u64::try_from(row_group.num_rows()).unwrap_or(0);
        row_count += row_group.num_rows();
        for (column_index, meta) in row_group.columns().iter().enumerate() {
            let stats = meta.statistics();
            let pages = metadata
                .offset_index()
                .and_then(|indexes| indexes.get(row_group_index))
                .and_then(|row| row.get(column_index));
            columns.push(ColumnMass {
                column: meta.column_path().string(),
                compressed_bytes: u64::try_from(meta.compressed_size()).unwrap_or(0),
                uncompressed_bytes: u64::try_from(meta.uncompressed_size()).unwrap_or(0),
                codec: meta.compression().to_string(),
                encodings: meta
                    .encodings()
                    .map(|encoding| encoding.to_string())
                    .collect(),
                num_values: u64::try_from(meta.num_values()).unwrap_or(0),
                dictionary: meta.dictionary_page_offset().is_some(),
                null_count: stats.and_then(Statistics::null_count_opt),
                distinct_count: stats.and_then(Statistics::distinct_count_opt),
                min_value: stats.and_then(|value| stat_text(value, true)),
                max_value: stats.and_then(|value| stat_text(value, false)),
                physical_type: meta.column_type().to_string(),
                row_group: u32::try_from(row_group_index).unwrap_or(0),
                row_group_rows,
                page_count: pages.map(|index| index.page_locations().len() as u64),
                page_compressed_bytes: pages.map(|index| {
                    index
                        .page_locations()
                        .iter()
                        .map(|page| u64::try_from(page.compressed_page_size).unwrap_or(0))
                        .sum()
                }),
            });
        }
    }
    let file = metadata.file_metadata();
    let key_values = file
        .key_value_metadata()
        .into_iter()
        .flatten()
        .map(|entry| {
            let value_bytes = entry.value.as_ref().map(String::len);
            let value = entry.value.as_ref().map(|value| {
                let mut end = value.len().min(256);
                while !value.is_char_boundary(end) {
                    end -= 1;
                }
                value[..end].to_owned()
            });
            MetadataEntry {
                key: entry.key.clone(),
                value,
                value_bytes,
                truncated: value_bytes.is_some_and(|size| size > 256),
            }
        })
        .collect();
    let row_groups = metadata
        .row_groups()
        .iter()
        .map(|group| {
            let sum = |compressed: bool| {
                group.columns().iter().try_fold(0u64, |total, column| {
                    let size = if compressed {
                        column.compressed_size()
                    } else {
                        column.uncompressed_size()
                    };
                    let size =
                        u64::try_from(size).map_err(|_| Error("negative column size".into()))?;
                    total
                        .checked_add(size)
                        .ok_or_else(|| Error("row-group size overflow".into()))
                })
            };
            Ok(RowGroupMetadata {
                row_count: u64::try_from(group.num_rows())
                    .map_err(|_| Error("negative row count".into()))?,
                compressed_bytes: sum(true)?,
                uncompressed_bytes: sum(false)?,
                column_indexes: group
                    .columns()
                    .iter()
                    .map(|column| column.column_index_offset().is_some())
                    .collect(),
                offset_indexes: group
                    .columns()
                    .iter()
                    .map(|column| column.offset_index_offset().is_some())
                    .collect(),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(FileMass {
        metadata: FileMetadata {
            creator: file.created_by().map(str::to_owned),
            format_version: file.version(),
            key_values,
            row_groups,
        },
        row_count: u64::try_from(row_count).unwrap_or(0),
        row_group_count: metadata.row_groups().len(),
        sorting_columns: sorting_columns(metadata),
        columns,
    })
}

fn sorting_columns(metadata: &ParquetMetaData) -> Vec<String> {
    let Some(row_group) = metadata.row_groups().first() else {
        return Vec::new();
    };
    let Some(columns) = row_group.sorting_columns() else {
        return Vec::new();
    };
    columns
        .iter()
        .filter_map(|column| {
            let path = row_group
                .columns()
                .get(usize::try_from(column.column_idx).ok()?)?
                .column_path()
                .string();
            Some(if column.descending {
                format!("{path} DESC")
            } else {
                path
            })
        })
        .collect()
}

fn stat_text(stats: &Statistics, min: bool) -> Option<String> {
    match stats {
        Statistics::Boolean(value) => pick(value, min).map(ToString::to_string),
        Statistics::Int32(value) => pick(value, min).map(ToString::to_string),
        Statistics::Int64(value) => pick(value, min).map(ToString::to_string),
        Statistics::Float(value) => pick(value, min).map(ToString::to_string),
        Statistics::Double(value) => pick(value, min).map(ToString::to_string),
        Statistics::ByteArray(value) => {
            pick(value, min).and_then(|value| bytes_text(value.as_bytes()))
        }
        Statistics::FixedLenByteArray(value) => {
            pick(value, min).and_then(|value| bytes_text(value.as_bytes()))
        }
        Statistics::Int96(_) => None,
    }
}

fn pick<T>(stats: &parquet::file::statistics::ValueStatistics<T>, min: bool) -> Option<&T> {
    if min {
        stats.min_opt()
    } else {
        stats.max_opt()
    }
}

fn bytes_text(value: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(value).ok()?;
    text.chars()
        .all(|ch| !ch.is_control())
        .then(|| text.to_string())
}

fn collect_pages(reader: Box<dyn PageReader>) -> Result<Vec<Page>, Error> {
    let mut out = Vec::new();
    for page in reader {
        let page = page?;
        out.push(create_page(page));
    }
    Ok(out)
}

fn create_page(page: ParquetPage) -> Page {
    Page {
        payload: page.buffer().to_vec(),
        value_count: page.num_values(),
        dictionary: page.is_dictionary_page(),
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Error(e.to_string())
    }
}
