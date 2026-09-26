//! Dump a sample of Parquet files named by a table.
//!
//! File selection (include/exclude/sample) is the caller's job; [`selection`]
//! holds the grammar. This module reads the named files — optionally only the
//! first row groups — as rows or as a copied Parquet file. The Parquet work
//! lives behind [`crate::third_party::parquet`]; this module never names the
//! `parquet` crate.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::third_party::object_store::is_remote;
use crate::third_party::parquet::{self, Error, FileRows, Source};

pub mod selection;
pub use selection::{keep, select, Sample};

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
        match Sample::parse(value) {
            Ok(Sample::ALL) => Ok(Self::ALL),
            Ok(Sample::First(count)) => Ok(Self::First(count)),
            Ok(Sample::Every(_)) => Err(Error(
                "row-groups does not support every:N; expected all or first:N".into(),
            )),
            Err(error) => Err(Error(error.to_string())),
        }
    }

    /// The per-file row-group limit, or `None` for every row group.
    fn limit(self) -> Option<usize> {
        match self {
            Self::ALL => None,
            Self::First(count) => Some(count as usize),
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
    pub env: BTreeMap<String, String>,
}

/// Arguments for [`dump`] and [`write_parquet`].
#[derive(Debug, Clone)]
pub struct DumpRequest {
    /// Files to read, in dump order.
    pub files: Vec<DumpFile>,
    /// Which row groups to take from each file.
    pub row_group: RowGroups,
}

/// Rows from one dump: a column list and one record per row.
#[derive(Debug, Clone)]
pub struct Dump {
    /// Column names, `_table` / `_path` first when those fields are present.
    pub columns: Vec<String>,
    /// Row values aligned to [`Dump::columns`].
    pub rows: Vec<Vec<Value>>,
}

/// Read rows from the named files, limited to [`DumpRequest::row_group`].
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
    let limit = request.row_group.limit();
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for file in &files {
        let env = env_pairs(&file.env);
        let source = Source {
            uri: &file.uri,
            env: &env,
        };
        let read = parquet::read_rows(&source, limit).await?;
        let (file_columns, file_rows) = label(file, read)?;
        merge_columns(&mut columns, &file_columns, &mut rows);
        for row in file_rows {
            rows.push(align(&columns, &file_columns, row));
        }
    }
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
    let envs: Vec<Vec<(String, String)>> = files.iter().map(|file| env_pairs(&file.env)).collect();
    let sources: Vec<Source<'_>> = files
        .iter()
        .zip(&envs)
        .map(|(file, env)| Source {
            uri: &file.uri,
            env,
        })
        .collect();
    parquet::write_parquet(&sources, request.row_group.limit()).await
}

/// Prepend the `_table` / `_path` columns to one file's rows.
fn label(file: &DumpFile, read: FileRows) -> Result<(Vec<String>, Vec<Vec<Value>>), Error> {
    for name in &read.columns {
        if name == "_path" || name == "_table" {
            return Err(Error(format!(
                "{} already has a `{name}` column; dump will not replace it",
                file.uri
            )));
        }
    }
    let mut columns = Vec::with_capacity(read.columns.len() + 2);
    if file.table.is_some() {
        columns.push("_table".into());
    }
    columns.push("_path".into());
    columns.extend(read.columns);
    let rows = read
        .rows
        .into_iter()
        .map(|row| {
            let mut values = Vec::with_capacity(row.len() + 2);
            if let Some(table) = &file.table {
                values.push(Value::String(table.clone()));
            }
            values.push(Value::String(file.path.clone()));
            values.extend(row);
            values
        })
        .collect();
    Ok((columns, rows))
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

fn env_pairs(env: &BTreeMap<String, String>) -> Vec<(String, String)> {
    env.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
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
