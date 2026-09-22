//! Dump sampled rows from Parquet files named by a table.
//!
//! File selection (include/exclude/sample) is the caller's job. This module
//! reads the named files and emits rows. The only module that names the
//! `parquet` crate is this one and `parquet_impl`.

use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use serde_json::{Map, Value};

use crate::object_store;
use crate::parquet_helpers::Error;

/// One file to dump. `path` is the log or input path; `uri` is what to read.
#[derive(Debug, Clone)]
pub struct DumpFile {
    /// Path as recorded in the table log, or the input path.
    pub path: String,
    /// URI or filesystem path to the Parquet object.
    pub uri: String,
    /// Lake table name, when the row comes from a lake.
    pub table: Option<String>,
}

/// Arguments for [`dump`].
#[derive(Debug, Clone)]
pub struct DumpRequest {
    /// Files to read, in dump order.
    pub files: Vec<DumpFile>,
}

/// Rows from one dump: a column list and one record per row.
#[derive(Debug, Clone)]
pub struct Dump {
    /// Column names, `_table` / `_path` first when those fields are present.
    pub columns: Vec<String>,
    /// Row values aligned to [`Dump::columns`].
    pub rows: Vec<Vec<Value>>,
}

/// Read every row from the named files.
///
/// Local paths are opened on disk. `s3://` URIs fetch the whole object through
/// `object_store` and need the `aws` feature.
///
/// # Errors
/// Fails when there are no files, a file cannot be read, or a row cannot be
/// decoded.
pub async fn dump(request: &DumpRequest) -> Result<Dump, Error> {
    if request.files.is_empty() {
        return Err(Error("no files".into()));
    }
    let files = expand_files(&request.files)?;
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for file in &files {
        let (file_columns, file_rows) = read_file(file).await?;
        merge_columns(&mut columns, &file_columns, &mut rows);
        for row in file_rows {
            rows.push(align(&columns, &file_columns, row));
        }
    }
    Ok(Dump { columns, rows })
}

/// Render the dump as CSV (header, then one row per line).
pub fn render_csv(dump: &Dump) -> String {
    let mut out = csv_line(&dump.columns);
    out.push('\n');
    for row in &dump.rows {
        let fields: Vec<String> = row.iter().map(csv_value).collect();
        out.push_str(&csv_line(&fields));
        out.push('\n');
    }
    out
}

/// Render the dump as NDJSON: one object per row.
///
/// # Errors
/// Fails when a row cannot be serialized.
pub fn render_json(dump: &Dump) -> Result<String, Error> {
    let mut out = String::new();
    for row in &dump.rows {
        let mut object = Map::new();
        for (column, value) in dump.columns.iter().zip(row) {
            object.insert(column.clone(), value.clone());
        }
        out.push_str(
            &serde_json::to_string(&Value::Object(object))
                .map_err(|error| Error(format!("cannot serialize dump row: {error}")))?,
        );
        out.push('\n');
    }
    Ok(out)
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

async fn read_file(file: &DumpFile) -> Result<(Vec<String>, Vec<Vec<Value>>), Error> {
    let bytes = read_bytes(&file.uri).await?;
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes))
        .map_err(|error| Error(format!("{}: {error}", file.uri)))?;
    let mut columns = Vec::new();
    if file.table.is_some() {
        columns.push("_table".into());
    }
    columns.push("_path".into());
    let mut rows = Vec::new();
    for record in reader
        .get_row_iter(None)
        .map_err(|error| Error(format!("{}: {error}", file.uri)))?
    {
        let record = record.map_err(|error| Error(format!("{}: {error}", file.uri)))?;
        let mut values = Vec::new();
        if let Some(table) = &file.table {
            values.push(Value::String(table.clone()));
        }
        values.push(Value::String(file.path.clone()));
        for (name, field) in record.get_column_iter() {
            if rows.is_empty() {
                columns.push(name.clone());
            }
            values.push(field_value(field));
        }
        rows.push(values);
    }
    Ok((columns, rows))
}

async fn read_bytes(uri: &str) -> Result<Vec<u8>, Error> {
    if is_remote(uri) {
        return read_remote(uri).await;
    }
    let path = local_path(uri);
    std::fs::read(&path).map_err(|error| Error(format!("cannot read {}: {error}", path.display())))
}

async fn read_remote(uri: &str) -> Result<Vec<u8>, Error> {
    let reader = object_store::open(uri, &[]).map_err(|error| Error(error.to_string()))?;
    let stat = reader
        .stat()
        .await
        .map_err(|error| Error(error.to_string()))?;
    reader
        .read_range(0..stat.size, stat.identity.as_deref())
        .await
        .map_err(|error| Error(error.to_string()))
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

fn field_value(field: &Field) -> Value {
    match field {
        Field::Null => Value::Null,
        Field::Bool(value) => Value::Bool(*value),
        Field::Byte(value) => Value::from(*value),
        Field::Short(value) => Value::from(*value),
        Field::Int(value) => Value::from(*value),
        Field::Long(value) => Value::from(*value),
        Field::UByte(value) => Value::from(*value),
        Field::UShort(value) => Value::from(*value),
        Field::UInt(value) => Value::from(*value),
        Field::ULong(value) => Value::from(*value),
        Field::Float(value) => serde_json::Number::from_f64(f64::from(*value))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::Double(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::Str(value) => Value::String(value.clone()),
        Field::Group(row) => {
            let mut object = Map::new();
            for (name, field) in row.get_column_iter() {
                object.insert(name.clone(), field_value(field));
            }
            Value::Object(object)
        }
        other => Value::String(other.to_string()),
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

fn csv_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn csv_line(fields: &[String]) -> String {
    fields
        .iter()
        .map(|field| {
            if field.contains([',', '"', '\n', '\r']) {
                format!("\"{}\"", field.replace('"', "\"\""))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}
