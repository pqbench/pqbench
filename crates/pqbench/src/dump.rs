//! Dump a sample of Parquet files named by a table.
//!
//! File selection (include/exclude/sample) is the caller's job. This module
//! reads the named files — optionally only the first row groups — and writes
//! Parquet. Copied pages keep their source encodings; newly written columns
//! default to zstd. The only module that names the `parquet` crate is this
//! one and `parquet_impl`.

use bytes::Bytes;
use parquet::basic::Compression;
use parquet::column::writer::ColumnCloseResult;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{ChunkReader, FileReader, Length, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::reader::RowIter;
use parquet::record::Field;
use serde_json::{Map, Value};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use crate::object_store;
use crate::parquet_helpers::Error;

/// How many leading row groups to read from each file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowGroups {
    /// Read every row group.
    ALL,
    /// Read the first `n` row groups (`n >= 1`).
    First(u32),
}

impl RowGroups {
    /// Parse `all` or `first:N`. The sample grammar also has `every:N`; that
    /// is files, not row groups.
    ///
    /// # Errors
    /// Fails when the spec is unknown or `N` is not a positive integer.
    pub fn parse(value: &str) -> Result<Self, Error> {
        match crate::pattern::Sample::parse(value) {
            Ok(crate::pattern::Sample::ALL) => Ok(Self::ALL),
            Ok(crate::pattern::Sample::First(count)) => Ok(Self::First(count)),
            Ok(crate::pattern::Sample::Every(_)) => Err(Error(
                "row-groups does not support every:N; expected all or first:N".into(),
            )),
            Err(error) => Err(Error(error.to_string())),
        }
    }

    fn keep(self, total: usize) -> usize {
        match self {
            Self::ALL => total,
            Self::First(count) => total.min(count as usize),
        }
    }
}

/// One file to dump. `path` is the log or input path; `uri` is what to read.
#[derive(Debug, Clone)]
pub struct DumpFile {
    /// Path as recorded in the table log, or the input path.
    pub path: String,
    /// URI or filesystem path to the Parquet object.
    pub uri: String,
    /// Lake table name, when the row comes from a lake.
    pub table: Option<String>,
    /// Storage options for this file (`AWS_*`).
    pub env: std::collections::BTreeMap<String, String>,
}

/// Arguments for [`dump`] and [`write_parquet`].
#[derive(Debug, Clone)]
pub struct DumpRequest {
    /// Files to read, in dump order.
    pub files: Vec<DumpFile>,
    /// Row groups to take from each file.
    pub row_groups: RowGroups,
}

/// Rows from one dump: a column list and one record per row.
#[derive(Debug, Clone)]
pub struct Dump {
    /// Column names, `_table` / `_path` first when those fields are present.
    pub columns: Vec<String>,
    /// Row values aligned to [`Dump::columns`].
    pub rows: Vec<Vec<Value>>,
}

/// Read rows from the named files, limited to [`DumpRequest::row_groups`].
///
/// Local files are seeked. `s3://` URIs fetch the footer and the selected row
/// groups, not the rest of the object, and need the `aws` feature.
///
/// # Errors
/// Fails when there are no files, a file cannot be read, or a row cannot be
/// decoded.
pub async fn dump(request: &DumpRequest) -> Result<Dump, Error> {
    let files = expand_files(&request.files)?;
    if files.is_empty() {
        return Err(Error("no files".into()));
    }
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for file in &files {
        let source = open_source(file, request.row_groups).await?;
        let keep = request.row_groups.keep(source.metadata.num_row_groups());
        let (file_columns, file_rows) = read_rows(file, &source, keep, true, None)?;
        merge_columns(&mut columns, &file_columns, &mut rows);
        for row in file_rows {
            rows.push(align(&columns, &file_columns, row));
        }
    }
    Ok(Dump { columns, rows })
}

/// Read data columns only (no `_path` / `_table`). Stops after `max_rows`.
///
/// # Errors
/// Fails when there are no files, a file cannot be read, or a row cannot be
/// decoded.
pub async fn sample(request: &DumpRequest, max_rows: Option<usize>) -> Result<Dump, Error> {
    let files = expand_files(&request.files)?;
    if files.is_empty() {
        return Err(Error("no files".into()));
    }
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for file in &files {
        if max_rows.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
        let remaining = max_rows.map(|limit| limit.saturating_sub(rows.len()));
        let source = open_source(file, request.row_groups).await?;
        let keep = request.row_groups.keep(source.metadata.num_row_groups());
        let (file_columns, file_rows) = read_rows(file, &source, keep, false, remaining)?;
        merge_columns(&mut columns, &file_columns, &mut rows);
        for row in file_rows {
            rows.push(align(&columns, &file_columns, row));
        }
    }
    Ok(Dump { columns, rows })
}

/// Read data columns from an in-memory Parquet file. Stops after `max_rows`.
///
/// # Errors
/// Fails when `bytes` is not a readable Parquet file.
pub fn sample_bytes(bytes: &[u8], max_rows: Option<usize>) -> Result<Dump, Error> {
    let reader = SerializedFileReader::new(bytes::Bytes::copy_from_slice(bytes))
        .map_err(|error| Error(error.to_string()))?;
    let file = DumpFile {
        path: "-".into(),
        uri: "-".into(),
        table: None,
        env: Default::default(),
    };
    let keep = reader.num_row_groups();
    let (columns, rows) = read_from_reader(&file, &reader, keep, false, max_rows)?;
    Ok(Dump { columns, rows })
}

/// Copy the selected row groups into one Parquet file, preserving encodings.
///
/// # Errors
/// Fails when there are no files, a file cannot be read, schemas differ, or
/// the writer cannot emit the file.
pub async fn write_parquet(request: &DumpRequest) -> Result<Vec<u8>, Error> {
    let files = expand_files(&request.files)?;
    if files.is_empty() {
        return Err(Error("no files".into()));
    }
    let first = open_source(&files[0], request.row_groups).await?;
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
        request.row_groups.keep(first.metadata.num_row_groups()),
    )?;
    for file in files.iter().skip(1) {
        let source = open_source(file, request.row_groups).await?;
        let file_schema = source
            .metadata
            .file_metadata()
            .schema_descr_ptr()
            .root_schema_ptr();
        if file_schema.as_ref() != schema.as_ref() {
            return Err(Error(format!(
                "{} has a different schema; dump parquet needs one schema",
                file.uri
            )));
        }
        append_groups(
            &mut writer,
            &source,
            request.row_groups.keep(source.metadata.num_row_groups()),
        )?;
    }
    writer.close().map_err(parquet_error)?;
    Ok(out)
}

fn writer_properties() -> parquet::file::properties::WriterPropertiesPtr {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build(),
    )
}

struct Source {
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
    fn part_from(&self, start: u64) -> parquet::errors::Result<Bytes> {
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
        Ok(Cursor::new(self.part_from(start)?))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let bytes = self.part_from(start)?;
        if bytes.len() < length {
            return Err(parquet::errors::ParquetError::General(format!(
                "byte range {start}+{length} was not fetched"
            )));
        }
        Ok(bytes.slice(..length))
    }
}

fn expand_files(files: &[DumpFile]) -> Result<Vec<DumpFile>, Error> {
    let mut expanded = Vec::new();
    for file in files {
        if has_glob(&file.uri) && !is_remote(&file.uri) {
            let mut matched = false;
            for entry in glob::glob(&file.uri)
                .map_err(|error| Error(format!("invalid mask {}: {error}", file.uri)))?
            {
                let path = entry
                    .map_err(|error| Error(format!("cannot expand mask {}: {error}", file.uri)))?;
                let uri = path.to_string_lossy().into_owned();
                expanded.push(DumpFile {
                    path: uri.clone(),
                    uri,
                    table: file.table.clone(),
                    env: file.env.clone(),
                });
                matched = true;
            }
            if !matched {
                return Err(Error(format!("mask matched no files: {}", file.uri)));
            }
        } else {
            expanded.push(file.clone());
        }
    }
    Ok(expanded)
}

fn has_glob(input: &str) -> bool {
    input.contains(['*', '?'])
}

async fn open_source(file: &DumpFile, row_groups: RowGroups) -> Result<Source, Error> {
    if is_remote(&file.uri) {
        return open_remote(file, row_groups).await;
    }
    let path = local_path(&file.uri);
    let file = File::open(&path)
        .map_err(|error| Error(format!("cannot read {}: {error}", path.display())))?;
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .map_err(parquet_error)?;
    Ok(Source {
        reader: SourceReader::Local(file),
        metadata,
    })
}

async fn open_remote(file: &DumpFile, row_groups: RowGroups) -> Result<Source, Error> {
    let uri = &file.uri;
    let options: Vec<(String, String)> = file
        .env
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let reader = object_store::open(uri, &options).map_err(|error| Error(error.to_string()))?;
    let stat = reader
        .stat()
        .await
        .map_err(|error| Error(error.to_string()))?;
    let size = stat.size;
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
    let keep = row_groups.keep(metadata.num_row_groups());
    let mut parts = vec![(metadata_start, Bytes::from(footer))];
    for range in data_ranges(&metadata, keep)? {
        let bytes = reader
            .read_range(range.clone(), identity)
            .await
            .map_err(|error| Error(error.to_string()))?;
        parts.push((range.start, Bytes::from(bytes)));
    }
    Ok(Source {
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

fn read_rows(
    file: &DumpFile,
    source: &Source,
    keep: usize,
    path_columns: bool,
    max_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<Vec<Value>>), Error> {
    let reader = SerializedFileReader::new(clone_reader(&source.reader)?)
        .map_err(|error| Error(format!("{}: {error}", file.uri)))?;
    read_from_reader(file, &reader, keep, path_columns, max_rows)
}

fn read_from_reader(
    file: &DumpFile,
    reader: &SerializedFileReader<impl ChunkReader + 'static>,
    keep: usize,
    path_columns: bool,
    max_rows: Option<usize>,
) -> Result<(Vec<String>, Vec<Vec<Value>>), Error> {
    let mut columns = Vec::new();
    if path_columns {
        if file.table.is_some() {
            columns.push("_table".into());
        }
        columns.push("_path".into());
    }
    let mut rows = Vec::new();
    for index in 0..keep {
        if max_rows.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
        let group = reader
            .get_row_group(index)
            .map_err(|error| Error(format!("{}: {error}", file.uri)))?;
        for record in RowIter::from_row_group(None, group.as_ref())
            .map_err(|error| Error(format!("{}: {error}", file.uri)))?
        {
            if max_rows.is_some_and(|limit| rows.len() >= limit) {
                break;
            }
            let record = record.map_err(|error| Error(format!("{}: {error}", file.uri)))?;
            let mut values = Vec::new();
            if path_columns {
                if let Some(table) = &file.table {
                    values.push(Value::String(table.clone()));
                }
                values.push(Value::String(file.path.clone()));
            }
            for (name, field) in record.get_column_iter() {
                if path_columns && (name == "_path" || name == "_table") {
                    return Err(Error(format!(
                        "{} already has a `{name}` column; dump will not replace it",
                        file.uri
                    )));
                }
                if rows.is_empty() {
                    columns.push(name.clone());
                }
                values.push(field_value(field, name)?);
            }
            rows.push(values);
        }
    }
    Ok((columns, rows))
}

fn append_groups<W: Write + Send>(
    writer: &mut SerializedFileWriter<W>,
    source: &Source,
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

fn is_remote(uri: &str) -> bool {
    uri.contains("://") && !uri.starts_with("file://")
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

fn merge_columns(columns: &mut Vec<String>, incoming: &[String], rows: &mut [Vec<Value>]) {
    for column in incoming {
        if !columns.iter().any(|name| name == column) {
            columns.push(column.clone());
            for row in rows.iter_mut() {
                row.push(Value::Null);
            }
        }
    }
}

fn align(columns: &[String], file_columns: &[String], row: Vec<Value>) -> Vec<Value> {
    columns
        .iter()
        .map(|column| {
            file_columns
                .iter()
                .position(|name| name == column)
                .and_then(|index| row.get(index).cloned())
                .unwrap_or(Value::Null)
        })
        .collect()
}
