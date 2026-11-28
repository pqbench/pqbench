//! The bundled parquet-rs-backed page parser.
//!
//! This module is private (`lib.rs` doesn't `pub mod` it); the only thing the
//! crate exposes is [`super::api::parse_pages`]. Tool code never imports
//! `parquet` directly.

use std::path::Path;
use std::sync::Arc;

use parquet::basic::{
    Compression, ConvertedType, Encoding, GzipLevel, LogicalType, PageType, Repetition, TimeType,
    TimeUnit, Type as PhysicalType, ZstdLevel,
};
use parquet::column::page::{Page as ParquetPage, PageReader};
use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::metadata::{
    PageEncodingStats, ParquetMetaData, ParquetMetaDataOptions, ParquetMetaDataReader,
    SortingColumn,
};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::file::writer::{SerializedColumnWriter, SerializedFileWriter};
use parquet::schema::types::{ColumnDescriptor, ColumnPath, Type};

use super::api::{
    ColumnChunk, ColumnMass, Error, FileMass, Kind, Page, ParquetFile, TypedColumn, Value,
    WriteOptions,
};

/// Read a parquet file from an in-memory byte slice and extract its pages.
///
/// No disk I/O: `bytes` is wrapped as an in-memory `ChunkReader` and every
/// page payload is copied out into the returned [`ParquetFile`].
pub(crate) fn parse_pages(bytes: &[u8]) -> Result<ParquetFile, Error> {
    let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(bytes))?;
    let mut file = ParquetFile { chunks: Vec::new() };
    for rg in 0..reader.num_row_groups() {
        let row_group = reader.get_row_group(rg)?;
        for col in 0..row_group.num_columns() {
            let meta = row_group.metadata().column(col);
            let column = meta.column_path().string();
            let compression = meta.compression();
            if compression != Compression::UNCOMPRESSED {
                return Err(Error::Unsupported(format!(
                    "only NONE (uncompressed) parquet is supported; column {column} is {compression}"
                )));
            }
            let pages = collect_pages(row_group.get_column_page_reader(col)?)?;
            file.chunks.push(ColumnChunk { column, pages });
        }
    }
    Ok(file)
}

/// Decode byte masses from a complete Parquet footer without reading pages.
pub(crate) fn read_footer_masses(footer: &[u8], size_bytes: u64) -> Result<FileMass, Error> {
    let metadata = footer_reader().parse_and_finish(&bytes::Bytes::copy_from_slice(footer))?;
    Ok(create_masses(&metadata, size_bytes))
}

/// A metadata reader that keeps the footer's page encoding statistics whole.
///
/// The crate's default folds them into a bitmask of the data pages' encodings;
/// the data page counts need each page type's count, which only the whole
/// list carries.
fn footer_reader() -> ParquetMetaDataReader {
    ParquetMetaDataReader::new().with_metadata_options(Some(
        ParquetMetaDataOptions::new().with_encoding_stats_as_mask(false),
    ))
}

/// Write typed columns and rows to an in-memory Parquet file.
pub(crate) fn write_parquet(
    columns: &[TypedColumn],
    rows: &[Vec<Value>],
    options: &WriteOptions,
) -> Result<Vec<u8>, Error> {
    if columns.is_empty() {
        return Err(Error::Invalid("no columns to write".into()));
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
            return Err(Error::Encoding(format!(
                "encoding `{encoding}` applies to no column in the sample"
            )));
        }
    }
    if let Some(page_size) = options.page_size {
        builder = builder.set_data_page_size_limit(page_size);
    }
    if let Some(length) = options.stats_truncate_length {
        builder = builder.set_statistics_truncate_length(Some(length));
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
                        Error::Invalid(format!("sorting column `{name}` is not in the sample"))
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
                .ok_or_else(|| Error::Invalid("writer ran out of columns".into()))?;
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
                .map_err(|error| Error::Parquet(Box::new(error)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Type::group_type_builder("schema")
        .with_fields(fields)
        .build()
        .map(Arc::new)
        .map_err(|error| Error::Parquet(Box::new(error)))
}

/// Parse a codec name and optional level into a Parquet `Compression`.
fn parse_compression(name: &str, level: Option<i32>) -> Result<Compression, Error> {
    match name {
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => {
            let level = u32::try_from(level.unwrap_or(6))
                .map_err(|_| Error::Invalid("gzip level must be non-negative".into()))?;
            GzipLevel::try_new(level)
                .map(Compression::GZIP)
                .map_err(|error| Error::Parquet(Box::new(error)))
        }
        "lz4" => Ok(Compression::LZ4_RAW),
        "zstd" => ZstdLevel::try_new(level.unwrap_or(3))
            .map(Compression::ZSTD)
            .map_err(|error| Error::Parquet(Box::new(error))),
        other => Err(Error::Invalid(format!(
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
            return Err(Error::Encoding(format!(
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

/// Read byte masses from a local file. One open: the size comes from the same
/// handle the footer is read from.
pub(crate) fn read_file_masses(path: &Path) -> Result<FileMass, Error> {
    let file = std::fs::File::open(path).map_err(Error::Io)?;
    let size_bytes = file.metadata().map_err(Error::Io)?.len();
    let metadata = footer_reader().parse_and_finish(&file)?;
    Ok(create_masses(&metadata, size_bytes))
}

/// Read one `ColumnMass` per column chunk from the parsed footer.
fn create_masses(metadata: &ParquetMetaData, size_bytes: u64) -> FileMass {
    let mut row_count = 0u64;
    let mut columns = Vec::new();
    for row_group in metadata.row_groups() {
        row_count += u64::try_from(row_group.num_rows()).unwrap_or(0);
        for meta in row_group.columns() {
            let stats = meta.statistics();
            let descriptor = meta.column_descr();
            // A writer that records no dictionary page may still leave the
            // offset field at zero: only a positive offset locates one.
            let dictionary_page_offset = meta.dictionary_page_offset().filter(|offset| *offset > 0);
            let data_page_offset = meta.data_page_offset();
            let dictionary_bytes = dictionary_page_offset
                .filter(|offset| data_page_offset > *offset)
                .and_then(|offset| u64::try_from(data_page_offset - offset).ok());
            let page_stats = meta.page_encoding_stats().map(Vec::as_slice);
            let page_counts = data_page_counts(page_stats);
            let encodings: Vec<Encoding> = meta.encodings().collect();
            columns.push(ColumnMass {
                column: meta.column_path().string(),
                physical_data_type: descriptor.physical_type().to_string(),
                logical_type: logical_type_text(descriptor),
                max_definition_level: u16::try_from(descriptor.max_def_level()).unwrap_or(0),
                max_repetition_level: u16::try_from(descriptor.max_rep_level()).unwrap_or(0),
                compressed_bytes: u64::try_from(meta.compressed_size()).unwrap_or(0),
                uncompressed_bytes: u64::try_from(meta.uncompressed_size()).unwrap_or(0),
                codec: codec_name(meta.compression()).to_string(),
                encodings: encodings.iter().map(ToString::to_string).collect(),
                data_encodings: data_page_encodings(page_stats),
                num_values: u64::try_from(meta.num_values()).unwrap_or(0),
                dictionary: chunk_is_dictionary_encoded(
                    dictionary_page_offset,
                    page_counts,
                    &encodings,
                ),
                dictionary_bytes,
                plain_data_page_count: page_counts.map(|counts| counts.plain_data_page_count),
                dictionary_data_page_count: page_counts
                    .map(|counts| counts.dictionary_data_page_count),
                page_index: meta.column_index_offset().is_some()
                    && meta.offset_index_offset().is_some(),
                unencoded_bytes: meta
                    .unencoded_byte_array_data_bytes()
                    .and_then(|bytes| u64::try_from(bytes).ok()),
                null_count: stats.and_then(Statistics::null_count_opt),
                distinct_count: stats.and_then(Statistics::distinct_count_opt),
                min_value: stats.and_then(|value| stat_text(value, true)),
                max_value: stats.and_then(|value| stat_text(value, false)),
            });
        }
    }
    FileMass {
        size_bytes,
        writer: metadata.file_metadata().created_by().map(str::to_owned),
        row_count,
        row_group_count: metadata.row_groups().len(),
        columns,
    }
}

/// The data pages a chunk's footer counts, split by how they encode values.
#[derive(Debug, Clone, Copy, Default)]
struct DataPageCounts {
    plain_data_page_count: u64,
    dictionary_data_page_count: u64,
}

/// Whether a chunk's footer marks it dictionary-encoded.
///
/// A positive dictionary page offset is the direct answer, but `parquet-mr`
/// 1.8.3 omits that field even when the chunk wrote a dictionary page and its
/// data pages use it. The footer still records the dictionary two other ways —
/// a dictionary-encoded data page count and a `PLAIN_DICTIONARY`/`RLE_DICTIONARY`
/// entry in the chunk's encoding set — and either one marks the chunk.
fn chunk_is_dictionary_encoded(
    dictionary_page_offset: Option<i64>,
    page_counts: Option<DataPageCounts>,
    encodings: &[Encoding],
) -> bool {
    dictionary_page_offset.is_some()
        || page_counts.is_some_and(|counts| counts.dictionary_data_page_count > 0)
        || encodings.iter().any(|encoding| {
            matches!(
                encoding,
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
            )
        })
}

/// The chunk's data pages by encoding, when the footer counts its pages.
///
/// Dictionary pages count as neither: they carry the dictionary, not values.
fn data_page_counts(stats: Option<&[PageEncodingStats]>) -> Option<DataPageCounts> {
    let stats = stats?;
    let mut counts = DataPageCounts::default();
    for stat in stats {
        if !matches!(stat.page_type, PageType::DATA_PAGE | PageType::DATA_PAGE_V2) {
            continue;
        }
        let count = u64::try_from(stat.count).unwrap_or(0);
        match stat.encoding {
            Encoding::PLAIN => counts.plain_data_page_count += count,
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
                counts.dictionary_data_page_count += count;
            }
            _ => {}
        }
    }
    Some(counts)
}

/// The encodings the chunk's data pages use, in the footer's order, when the
/// footer counts its pages.
fn data_page_encodings(stats: Option<&[PageEncodingStats]>) -> Option<Vec<String>> {
    let stats = stats?;
    let mut encodings: Vec<String> = Vec::new();
    for stat in stats {
        if !matches!(stat.page_type, PageType::DATA_PAGE | PageType::DATA_PAGE_V2) {
            continue;
        }
        let encoding = stat.encoding.to_string();
        if !encodings.contains(&encoding) {
            encodings.push(encoding);
        }
    }
    (!encodings.is_empty()).then_some(encodings)
}

/// The leaf column's logical type, as the Parquet spec spells it, falling back
/// to the legacy converted type a pre-2.4.0 writer recorded instead.
fn logical_type_text(descriptor: &ColumnDescriptor) -> Option<String> {
    if let Some(logical) = descriptor.logical_type_ref() {
        return Some(logical_type_name(logical));
    }
    let converted = descriptor.converted_type();
    (converted != ConvertedType::NONE).then(|| converted.to_string())
}

/// `STRING`, `DECIMAL(10,2)`, `TIMESTAMP(MICROS,UTC)`, …, as the spec names them.
fn logical_type_name(logical: &LogicalType) -> String {
    match logical {
        LogicalType::String => "STRING".to_string(),
        LogicalType::Map => "MAP".to_string(),
        LogicalType::List => "LIST".to_string(),
        LogicalType::Enum => "ENUM".to_string(),
        LogicalType::Decimal(decimal) => {
            format!("DECIMAL({},{})", decimal.precision, decimal.scale)
        }
        LogicalType::Date => "DATE".to_string(),
        LogicalType::Time(time) => format!("TIME({})", time_text(time)),
        LogicalType::Timestamp(timestamp) => format!("TIMESTAMP({})", time_text(timestamp)),
        LogicalType::Integer(integer) => {
            format!("INT({},{})", integer.bit_width, integer.is_signed)
        }
        LogicalType::Unknown => "UNKNOWN".to_string(),
        LogicalType::Json => "JSON".to_string(),
        LogicalType::Bson => "BSON".to_string(),
        LogicalType::Uuid => "UUID".to_string(),
        LogicalType::Float16 => "FLOAT16".to_string(),
        LogicalType::Variant(_) => "VARIANT".to_string(),
        LogicalType::Geometry(_) => "GEOMETRY".to_string(),
        LogicalType::Geography(_) => "GEOGRAPHY".to_string(),
        LogicalType::_Unknown { field_id } => format!("UNKNOWN({field_id})"),
    }
}

/// The time unit, and whether the value is adjusted to UTC.
fn time_text(time: &TimeType) -> String {
    let zone = if time.is_adjusted_to_u_t_c {
        "UTC"
    } else {
        "LOCAL"
    };
    format!("{},{zone}", time_unit_text(time.unit))
}

/// The time unit's name as the spec spells it.
fn time_unit_text(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::MILLIS => "MILLIS",
        TimeUnit::MICROS => "MICROS",
        TimeUnit::NANOS => "NANOS",
    }
}

/// The Parquet codec family name without parquet-rs's default level placeholders
/// (Parquet footers do not record compression levels).
fn codec_name(compression: Compression) -> &'static str {
    match compression {
        Compression::UNCOMPRESSED => "UNCOMPRESSED",
        Compression::SNAPPY => "SNAPPY",
        Compression::GZIP(_) => "GZIP",
        Compression::LZO => "LZO",
        Compression::BROTLI(_) => "BROTLI",
        Compression::LZ4 => "LZ4",
        Compression::ZSTD(_) => "ZSTD",
        Compression::LZ4_RAW => "LZ4_RAW",
    }
}

fn stat_text(stats: &Statistics, min: bool) -> Option<String> {
    match stats {
        Statistics::Boolean(value) => pick(value, min).map(ToString::to_string),
        Statistics::Int32(value) => pick(value, min).map(ToString::to_string),
        Statistics::Int64(value) => pick(value, min).map(ToString::to_string),
        Statistics::Float(value) => pick(value, min).map(ToString::to_string),
        Statistics::Double(value) => pick(value, min).map(ToString::to_string),
        Statistics::ByteArray(value) => pick(value, min).and_then(|value| bytes_text(value.data())),
        Statistics::FixedLenByteArray(value) => {
            pick(value, min).and_then(|value| bytes_text(value.data()))
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
        Error::Parquet(Box::new(e))
    }
}

#[cfg(test)]
mod tests {
    use parquet::file::metadata::{
        ColumnChunkMetaData, FileMetaData, ParquetMetaDataWriter, RowGroupMetaData,
    };
    use parquet::schema::types::SchemaDescriptor;

    use super::*;

    /// A footer of one `BYTE_ARRAY` chunk with the requested dictionary page
    /// offset, encoding set, and page encoding counts.
    fn dictionary_footer(
        dictionary_page_offset: Option<i64>,
        encodings: Vec<Encoding>,
        page_stats: Option<Vec<PageEncodingStats>>,
    ) -> Vec<u8> {
        let leaf = Type::primitive_type_builder("s", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .build()
            .expect("a valid leaf column");
        let schema = Type::group_type_builder("schema")
            .with_fields(vec![Arc::new(leaf)])
            .build()
            .expect("a valid schema");
        let schema_descriptor = Arc::new(SchemaDescriptor::new(Arc::new(schema)));
        let mut chunk = ColumnChunkMetaData::builder(schema_descriptor.column(0))
            .set_compression(Compression::UNCOMPRESSED)
            .set_encodings(encodings)
            .set_num_values(400)
            .set_total_compressed_size(1_000)
            .set_total_uncompressed_size(2_000)
            .set_data_page_offset(500);
        if let Some(offset) = dictionary_page_offset {
            chunk = chunk.set_dictionary_page_offset(Some(offset));
        }
        if let Some(stats) = page_stats {
            chunk = chunk.set_page_encoding_stats(stats);
        }
        let chunk = chunk.build().expect("a valid chunk");
        let row_group = RowGroupMetaData::builder(schema_descriptor.clone())
            .set_num_rows(400)
            .set_total_byte_size(1_000)
            .add_column_metadata(chunk)
            .build()
            .expect("a valid row group");
        let file_metadata = FileMetaData::new(
            1,
            400,
            Some("test writer".to_string()),
            None,
            schema_descriptor,
            None,
        );
        let metadata = ParquetMetaData::new(file_metadata, vec![row_group]);
        let mut footer = Vec::new();
        ParquetMetaDataWriter::new(&mut footer, &metadata)
            .finish()
            .expect("a written footer");
        footer
    }

    /// The page encoding counts of a chunk that kept one dictionary page and
    /// wrote two dictionary-encoded data pages beside three PLAIN ones.
    fn page_stats() -> Vec<PageEncodingStats> {
        vec![
            PageEncodingStats {
                page_type: PageType::DICTIONARY_PAGE,
                encoding: Encoding::PLAIN,
                count: 1,
            },
            PageEncodingStats {
                page_type: PageType::DATA_PAGE_V2,
                encoding: Encoding::RLE_DICTIONARY,
                count: 2,
            },
            PageEncodingStats {
                page_type: PageType::DATA_PAGE,
                encoding: Encoding::PLAIN,
                count: 3,
            },
        ]
    }

    #[test]
    fn footer_facts_come_from_the_footer_alone() {
        let footer = dictionary_footer(
            Some(0),
            vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY],
            Some(page_stats()),
        );
        let mass = read_footer_masses(&footer, footer.len() as u64).expect("a readable footer");

        assert_eq!(mass.row_count, 400);
        assert_eq!(mass.row_group_count, 1);
        assert_eq!(mass.writer.as_deref(), Some("test writer"));
        let column = &mass.columns[0];
        assert_eq!(column.column, "s");
        assert_eq!(column.logical_type.as_deref(), Some("STRING"));
        assert_eq!(column.plain_data_page_count, Some(3));
        assert_eq!(column.dictionary_data_page_count, Some(2));
        assert_eq!(
            column.data_encodings,
            Some(vec!["RLE_DICTIONARY".to_string(), "PLAIN".to_string()])
        );
    }

    /// A positive offset locates the dictionary page and its size.
    #[test]
    fn a_dictionary_page_offset_bounds_the_dictionary_page() {
        let footer = dictionary_footer(Some(100), vec![Encoding::PLAIN], None);
        let mass = read_footer_masses(&footer, footer.len() as u64).expect("a readable footer");

        let column = &mass.columns[0];
        assert!(column.dictionary);
        assert_eq!(column.dictionary_bytes, Some(400));
    }

    /// parquet-mr 1.8.3 writes dictionary pages and omits the offset: the page
    /// counts still record them, and a zero offset locates nothing to size.
    #[test]
    fn an_omitted_dictionary_page_offset_with_dictionary_pages_is_a_dictionary_chunk() {
        let footer = dictionary_footer(
            None,
            vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY],
            Some(page_stats()),
        );
        let mass = read_footer_masses(&footer, footer.len() as u64).expect("a readable footer");

        let column = &mass.columns[0];
        assert!(
            column.dictionary,
            "dictionary data pages mark the chunk, offset or not"
        );
        assert!(
            column.dictionary_bytes.is_none(),
            "an omitted offset bounds no dictionary page"
        );
    }

    /// The chunk's encoding set is the other footer fact that marks a
    /// dictionary-encoded chunk whose page counts are absent.
    #[test]
    fn a_dictionary_encoding_without_an_offset_or_page_counts_is_a_dictionary_chunk() {
        let footer = dictionary_footer(
            None,
            vec![Encoding::PLAIN, Encoding::PLAIN_DICTIONARY],
            None,
        );
        let mass = read_footer_masses(&footer, footer.len() as u64).expect("a readable footer");

        let column = &mass.columns[0];
        assert!(column.dictionary, "PLAIN_DICTIONARY marks the chunk");
        assert!(column.dictionary_bytes.is_none());
    }

    /// A PLAIN-only chunk with no offset and no page counts is not a
    /// dictionary chunk: the fallback detection must not invent one.
    #[test]
    fn a_plain_chunk_is_not_a_dictionary_chunk() {
        let footer = dictionary_footer(None, vec![Encoding::PLAIN], None);
        let mass = read_footer_masses(&footer, footer.len() as u64).expect("a readable footer");

        assert!(!mass.columns[0].dictionary);
    }
}
