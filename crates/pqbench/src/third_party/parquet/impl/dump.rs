//! The Parquet-backed `dump` implementation: read rows and copy row groups.
//!
//! This is the only dump file that names the `parquet` crate. It opens local
//! and remote sources, reads records into plain JSON values, and copies row
//! groups page-for-page. Selection and orchestration live in [`crate::dump`].

use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::Compression;
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use parquet::file::properties::{WriterProperties, WriterPropertiesPtr};
use parquet::file::reader::{ChunkReader, FileReader, Length, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::reader::RowIter;
use parquet::record::Field;
use serde_json::{Map, Value};

use super::super::api::{Error, FileRows, Source};
use crate::third_party::object_store;

/// Read rows from one source, keeping at most the first `row_groups`.
pub(crate) async fn read_rows(
    source: &Source<'_>,
    row_groups: Option<usize>,
) -> Result<FileRows, Error> {
    let opened = open_source(source, row_groups).await?;
    let keep = clamp(row_groups, opened.metadata.num_row_groups());
    let reader = SerializedFileReader::new(clone_reader(&opened.reader)?)
        .map_err(|error| Error(format!("{}: {error}", source.uri)))?;
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for index in 0..keep {
        let group = reader
            .get_row_group(index)
            .map_err(|error| Error(format!("{}: {error}", source.uri)))?;
        for record in RowIter::from_row_group(None, group.as_ref())
            .map_err(|error| Error(format!("{}: {error}", source.uri)))?
        {
            let record = record.map_err(|error| Error(format!("{}: {error}", source.uri)))?;
            let mut values = Vec::new();
            for (name, field) in record.get_column_iter() {
                if rows.is_empty() {
                    columns.push(name.clone());
                }
                values.push(field_value(field, name)?);
            }
            rows.push(values);
        }
    }
    Ok(FileRows { columns, rows })
}

/// Copy the selected row groups of every source into one Parquet buffer.
pub(crate) async fn write_parquet(
    sources: &[Source<'_>],
    row_groups: Option<usize>,
) -> Result<Vec<u8>, Error> {
    let Some(first_source) = sources.first() else {
        return Err(Error("no files".into()));
    };
    let first = open_source(first_source, row_groups).await?;
    let schema = first
        .metadata
        .file_metadata()
        .schema_descr_ptr()
        .root_schema_ptr();
    let mut out = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut out, schema.clone(), writer_properties())
        .map_err(parquet_error)?;
    append_groups(
        &mut writer,
        &first,
        clamp(row_groups, first.metadata.num_row_groups()),
    )?;
    for source in &sources[1..] {
        let opened = open_source(source, row_groups).await?;
        let file_schema = opened
            .metadata
            .file_metadata()
            .schema_descr_ptr()
            .root_schema_ptr();
        if file_schema.as_ref() != schema.as_ref() {
            return Err(Error(format!(
                "{} has a different schema; dump parquet needs one schema",
                source.uri
            )));
        }
        append_groups(
            &mut writer,
            &opened,
            clamp(row_groups, opened.metadata.num_row_groups()),
        )?;
    }
    writer.close().map_err(parquet_error)?;
    Ok(out)
}

/// The per-file row-group limit: every group when `row_groups` is `None`.
fn clamp(row_groups: Option<usize>, total: usize) -> usize {
    row_groups.map_or(total, |count| total.min(count))
}

fn writer_properties() -> WriterPropertiesPtr {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build(),
    )
}

struct Opened {
    reader: SourceReader,
    metadata: ParquetMetaData,
}

enum SourceReader {
    Local(File),
    Partial(PartialFile),
}

impl Length for SourceReader {
    fn len(&self) -> u64 {
        match self {
            Self::Local(file) => file.len(),
            Self::Partial(file) => file.len(),
        }
    }
}

impl ChunkReader for SourceReader {
    type T = Box<dyn Read + Send>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        match self {
            Self::Local(file) => Ok(Box::new(file.get_read(start)?)),
            Self::Partial(file) => Ok(Box::new(file.get_read(start)?)),
        }
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        match self {
            Self::Local(file) => file.get_bytes(start, length),
            Self::Partial(file) => file.get_bytes(start, length),
        }
    }
}

struct PartialFile {
    len: u64,
    parts: Vec<(u64, Bytes)>,
}

impl Length for PartialFile {
    fn len(&self) -> u64 {
        self.len
    }
}

impl PartialFile {
    fn part_bytes(&self, start: u64) -> parquet::errors::Result<Bytes> {
        for (offset, bytes) in &self.parts {
            let end = *offset + bytes.len() as u64;
            if start >= *offset && start < end {
                return Ok(bytes.slice((start - offset) as usize..));
            }
        }
        Err(parquet::errors::ParquetError::General(format!(
            "byte {start} was not fetched"
        )))
    }
}

impl ChunkReader for PartialFile {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(Cursor::new(self.part_bytes(start)?))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let bytes = self.part_bytes(start)?;
        if bytes.len() < length {
            return Err(parquet::errors::ParquetError::General(format!(
                "byte range {start}+{length} was not fetched"
            )));
        }
        Ok(bytes.slice(..length))
    }
}

async fn open_source(source: &Source<'_>, row_groups: Option<usize>) -> Result<Opened, Error> {
    if object_store::is_remote(source.uri) {
        return open_remote(source, row_groups).await;
    }
    let path = local_path(source.uri);
    let file = File::open(&path)
        .map_err(|error| Error(format!("cannot read {}: {error}", path.display())))?;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .map_err(parquet_error)?;
    Ok(Opened {
        reader: SourceReader::Local(file),
        metadata,
    })
}

async fn open_remote(source: &Source<'_>, row_groups: Option<usize>) -> Result<Opened, Error> {
    let uri = source.uri;
    let reader = object_store::open(uri, source.env).map_err(|error| Error(error.to_string()))?;
    let stat = reader
        .stat()
        .await
        .map_err(|error| Error(error.to_string()))?;
    let size = stat.size_bytes;
    if size < 8 {
        return Err(Error(format!(
            "object {uri} is too small to be a Parquet file: {size} bytes"
        )));
    }
    let identity = stat.identity.as_deref();
    let trailer = reader
        .read_range(size - 8..size, identity)
        .await
        .map_err(|error| Error(error.to_string()))?;
    let trailer: [u8; 8] = trailer
        .as_slice()
        .try_into()
        .map_err(|_| Error(format!("object {uri} returned a truncated Parquet trailer")))?;
    if &trailer[4..] != b"PAR1" {
        return Err(Error(format!("object {uri} has no Parquet footer magic")));
    }
    let metadata_size = u64::from(u32::from_le_bytes([
        trailer[0], trailer[1], trailer[2], trailer[3],
    ]));
    let metadata_start = size
        .checked_sub(8 + metadata_size)
        .filter(|start| *start >= 4)
        .ok_or_else(|| Error(format!("object {uri} has an invalid Parquet footer size")))?;
    let footer = reader
        .read_range(metadata_start..size, identity)
        .await
        .map_err(|error| Error(error.to_string()))?;
    if footer.len() as u64 != size - metadata_start {
        return Err(Error(format!(
            "object {uri} returned truncated Parquet metadata"
        )));
    }
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&Bytes::from(footer.clone()))
        .map_err(parquet_error)?;
    let keep = clamp(row_groups, metadata.num_row_groups());
    let mut parts = vec![(metadata_start, Bytes::from(footer))];
    for range in data_ranges(&metadata, keep)? {
        let bytes = reader
            .read_range(range.clone(), identity)
            .await
            .map_err(|error| Error(error.to_string()))?;
        parts.push((range.start, Bytes::from(bytes)));
    }
    Ok(Opened {
        reader: SourceReader::Partial(PartialFile { len: size, parts }),
        metadata,
    })
}

fn data_ranges(metadata: &ParquetMetaData, keep: usize) -> Result<Vec<Range<u64>>, Error> {
    let mut ranges = Vec::new();
    for group in metadata.row_groups().iter().take(keep) {
        ranges.extend(group_range(group)?);
    }
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::new();
    for range in ranges {
        match merged.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }
    Ok(merged)
}

fn group_range(group: &RowGroupMetaData) -> Result<Vec<Range<u64>>, Error> {
    let mut ranges = Vec::new();
    for column in group.columns() {
        let start = column
            .dictionary_page_offset()
            .unwrap_or_else(|| column.data_page_offset());
        let start = u64::try_from(start)
            .map_err(|_| Error("parquet column offset does not fit in u64".into()))?;
        let size = u64::try_from(column.compressed_size())
            .map_err(|_| Error("parquet column size does not fit in u64".into()))?;
        let end = start
            .checked_add(size)
            .ok_or_else(|| Error("parquet column range overflowed".into()))?;
        ranges.push(start..end);
    }
    Ok(ranges)
}

fn append_groups<W: Write + Send>(
    writer: &mut SerializedFileWriter<W>,
    source: &Opened,
    keep: usize,
) -> Result<(), Error> {
    for index in 0..keep {
        let group = source.metadata.row_group(index);
        let mut out = writer.next_row_group().map_err(parquet_error)?;
        for column in group.columns() {
            let close = ColumnCloseResult {
                bytes_written: u64::try_from(column.compressed_size())
                    .map_err(|_| Error("parquet column size does not fit in u64".into()))?,
                rows_written: u64::try_from(group.num_rows())
                    .map_err(|_| Error("parquet row count does not fit in u64".into()))?,
                metadata: column.clone(),
                bloom_filter: None,
                column_index: None,
                offset_index: None,
            };
            out.append_column(&source.reader, close)
                .map_err(parquet_error)?;
        }
        out.close().map_err(parquet_error)?;
    }
    Ok(())
}

fn clone_reader(reader: &SourceReader) -> Result<SourceReader, Error> {
    match reader {
        SourceReader::Local(file) => {
            Ok(SourceReader::Local(file.try_clone().map_err(|error| {
                Error(format!("cannot reopen parquet file: {error}"))
            })?))
        }
        SourceReader::Partial(file) => Ok(SourceReader::Partial(PartialFile {
            len: file.len,
            parts: file.parts.clone(),
        })),
    }
}

fn local_path(uri: &str) -> std::path::PathBuf {
    if let Some(path) = uri.strip_prefix("file://") {
        return Path::new(path).to_path_buf();
    }
    Path::new(uri).to_path_buf()
}

fn parquet_error(error: parquet::errors::ParquetError) -> Error {
    Error(error.to_string())
}

fn field_value(field: &Field, name: &str) -> Result<Value, Error> {
    match field {
        Field::Null => Ok(Value::Null),
        Field::Bool(value) => Ok(Value::Bool(*value)),
        Field::Byte(value) => Ok(Value::from(*value)),
        Field::Short(value) => Ok(Value::from(*value)),
        Field::Int(value) => Ok(Value::from(*value)),
        Field::Long(value) => Ok(Value::from(*value)),
        Field::UByte(value) => Ok(Value::from(*value)),
        Field::UShort(value) => Ok(Value::from(*value)),
        Field::UInt(value) => Ok(Value::from(*value)),
        Field::ULong(value) => Ok(Value::from(*value)),
        Field::Float(value) => serde_json::Number::from_f64(f64::from(*value))
            .map(Value::Number)
            .ok_or_else(|| Error(format!("column `{name}` is not a finite float"))),
        Field::Double(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| Error(format!("column `{name}` is not a finite float"))),
        Field::Str(value) => Ok(Value::String(value.clone())),
        Field::Group(row) => {
            let mut object = Map::new();
            for (child, field) in row.get_column_iter() {
                object.insert(child.clone(), field_value(field, child)?);
            }
            Ok(Value::Object(object))
        }
        other => Ok(Value::String(other.to_string())),
    }
}
