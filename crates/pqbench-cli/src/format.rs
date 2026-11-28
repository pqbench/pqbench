//! `format`: render a record stream as tables, or a `summarize columns` stream
//! as an HTML treemap.
//!
//! The walk is NDJSON end to end; this is the one command that turns a stream
//! back into something a human reads. Records are typed by their `name` — the
//! model's identity (AIP-122) — so a `catalogs/…/schemas/…` line is a `Schema`
//! and takes its columns from [`crate::render`]. A summarized column has no
//! name, so its shape is what places it. A record the curated type cannot hold,
//! or whose name has no columns yet, still renders from its own keys; a record
//! with no name and no summarized columns (a `diff` delta) is not table data.
//!
//! `--html` reads `ColumnMass` rows and writes the treemap page to standard
//! output instead.

use clap::Args;
use futures_util::StreamExt;
use pqbench::{diff, mass, v1};
use serde::Deserialize as _;
use serde_json::Value;

use crate::document::{self, Input, Record};
use crate::render::{self, Row};
use crate::source;
use crate::CliError;

/// Arguments for `format`.
#[derive(Args)]
pub(crate) struct FormatArgs {
    /// write an HTML treemap for a summarize columns stream
    #[arg(long)]
    html: bool,
}

/// What a record is, for grouping.
#[derive(Clone, PartialEq, Eq)]
enum Class {
    Metastore,
    Catalog,
    Schema,
    Table,
    TableFile,
    PartitionPrice,
    Partition,
    Column,
    Sweep,
    CompressionColumn,
    Profile,
    Suggestion,
    /// A summarized table column: no name, recognized by its shape.
    Mass,
    /// A `diff` column delta: no name, recognized by its shape.
    Delta,
    /// A record with no curated columns, grouped by its key set.
    Other(String),
}

pub(crate) async fn run(args: &FormatArgs) -> Result<(), CliError> {
    if args.html {
        return html().await;
    }
    tables().await
}

/// The `--html` page: only the `summarize columns` rows are supported.
async fn html() -> Result<(), CliError> {
    let mut rows = Vec::new();
    let mut records = document::records(Input::Stdin).await?;
    while let Some(record) = records.next().await {
        match record? {
            Record::Mass(row) => rows.push(row),
            Record::Blob | Record::Column(_) => {
                return Err(
                    "format --html reads a summarize columns stream; run `pqbench summarize columns` first"
                        .into(),
                );
            }
            Record::File(file) => {
                return Err(format!(
                    "format --html reads a summarize columns stream, not `{}`; run `pqbench list columns` and `pqbench summarize columns` first",
                    file.name
                )
                .into());
            }
        }
    }
    if rows.is_empty() {
        return Err("summarize columns stream has no rows".into());
    }
    render::write(&pqbench::viz::render_rows(&rows)?).await
}

/// The default: one table per record kind.
async fn tables() -> Result<(), CliError> {
    const COMMAND: &str = "format";
    let input = source::read_input(COMMAND).await?;
    let mut records = source::records(COMMAND, input.first, input.lines);
    let mut sections: Vec<(Class, Section)> = Vec::new();
    while let Some(record) = records.next().await {
        let record = record?;
        let Some(class) = classify(&record) else {
            continue;
        };
        if let Some(record) = section(&mut sections, class).push(record) {
            // The curated type did not fit the record; its keys still render.
            let fallback = Class::Other(signature(&record));
            section(&mut sections, fallback).push_values(record);
        }
    }
    let mut out = String::new();
    for (_, section) in &sections {
        let table = section.render();
        if table.is_empty() {
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&table);
    }
    render::write(&out).await
}

/// The section for `class`, created in first-seen order.
fn section(sections: &mut Vec<(Class, Section)>, class: Class) -> &mut Section {
    let at = match sections.iter().position(|(seen, _)| *seen == class) {
        Some(at) => at,
        None => {
            let at = sections.len();
            let section = Section::new(&class);
            sections.push((class, section));
            at
        }
    };
    &mut sections[at].1
}

/// A record's class: the resource its `name` names, the row kind, or the shape
/// of a summarized column.
fn classify(record: &Value) -> Option<Class> {
    let Some(name) = record.get("name").and_then(Value::as_str) else {
        if mass::ColumnMass::from_document(record).is_some() {
            return Some(Class::Mass);
        }
        if record.get("delta_bytes").is_some() && diff::ColumnDelta::deserialize(record).is_ok() {
            return Some(Class::Delta);
        }
        return None;
    };
    Some(if is_suggestion(record, name) {
        Class::Suggestion
    } else if v1::Column::parse_name(name).is_some() {
        Class::Column
    } else if name.contains("/codecs/") {
        Class::Sweep
    } else if name.ends_with("/profile") {
        Class::Profile
    } else if name.contains("/compression/") && name.contains("/columns/") {
        Class::CompressionColumn
    } else if name.contains("/compression/") {
        Class::Sweep
    } else if v1::name::parse_blob(name).is_some() {
        // A blob is a structural record: it exists so the next stage can read
        // storage, not as a table row.
        return None;
    } else if v1::name::parse_table_file(name).is_some() {
        Class::TableFile
    } else if v1::PartitionPrice::parse_name(name).is_some() {
        Class::PartitionPrice
    } else if v1::Partition::parse_name(name).is_some() {
        Class::Partition
    } else if v1::Table::parse_name(name).is_some() {
        Class::Table
    } else if v1::Schema::parse_name(name).is_some() {
        Class::Schema
    } else if v1::Catalog::parse_name(name).is_some() {
        Class::Catalog
    } else if v1::name::parse_metastore(name).is_some() {
        Class::Metastore
    } else {
        Class::Other(signature(record))
    })
}

/// A suggestion names its column and its rule, so a `…/suggestions/…` name
/// with both fields is one.
fn is_suggestion(record: &Value, name: &str) -> bool {
    name.contains("/suggestions/")
        && record.get("rule").and_then(Value::as_str).is_some()
        && record.get("column").and_then(Value::as_str).is_some()
}

/// A record's keys, in a stable order: records with the same shape share a
/// table.
fn signature(record: &Value) -> String {
    let Some(map) = record.as_object() else {
        return String::new();
    };
    let mut keys: Vec<&str> = map.keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys.join(",")
}

/// One table's rows: curated cells, or JSON records with no columns yet.
enum Section {
    /// Rows of one curated type, already reduced to cells.
    Curated {
        header: &'static [&'static str],
        align: &'static [render::Align],
        /// How a record joins the section: [`Self::curated`] names the type.
        extract: fn(Value) -> Result<Vec<String>, Value>,
        rows: Vec<Vec<String>>,
    },
    Other(Vec<Value>),
}

impl Section {
    /// The empty section for `class`; this match is the one place a curated
    /// record type is named.
    fn new(class: &Class) -> Self {
        match class {
            Class::Metastore => Self::curated::<v1::Metastore>(),
            Class::Catalog => Self::curated::<v1::Catalog>(),
            Class::Schema => Self::curated::<v1::Schema>(),
            Class::Table => Self::curated::<v1::Table>(),
            Class::TableFile => Self::curated::<v1::TableFile>(),
            Class::PartitionPrice => Self::curated::<v1::PartitionPrice>(),
            Class::Partition => Self::curated::<v1::Partition>(),
            Class::Column => Self::curated::<v1::Column>(),
            Class::Sweep => Self::curated::<render::SweepRow>(),
            Class::CompressionColumn => Self::curated::<render::ColumnSweepRow>(),
            Class::Profile => Self::curated::<render::ProfileRow>(),
            Class::Suggestion => Self::curated::<v1::Suggestion>(),
            Class::Mass => Self::curated::<mass::ColumnMass>(),
            Class::Delta => Self::curated::<diff::ColumnDelta>(),
            Class::Other(_) => Self::Other(Vec::new()),
        }
    }

    /// An empty section whose rows are `T`'s columns.
    fn curated<T: Row + serde::de::DeserializeOwned>() -> Self {
        Self::Curated {
            header: T::HEADER,
            align: T::ALIGN,
            extract: extract_row::<T>,
            rows: Vec::new(),
        }
    }

    /// Take the record, or give it back when the curated type cannot hold it.
    fn push(&mut self, record: Value) -> Option<Value> {
        match self {
            Self::Curated { rows, extract, .. } => match extract(record) {
                Ok(cells) => {
                    rows.push(cells);
                    None
                }
                Err(record) => Some(record),
            },
            Self::Other(rows) => {
                rows.push(record);
                None
            }
        }
    }

    /// Keep a record the curated types cannot hold.
    fn push_values(&mut self, record: Value) {
        if let Self::Other(rows) = self {
            rows.push(record);
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Curated {
                header,
                align,
                rows,
                ..
            } => render::table_cells(header, align, rows),
            Self::Other(rows) => render::table_values(rows),
        }
    }
}

/// Deserialize into the curated type and reduce it to cells; a record the type
/// cannot hold comes back for the generic table, so nothing is dropped
/// silently.
fn extract_row<T: Row + serde::de::DeserializeOwned>(record: Value) -> Result<Vec<String>, Value> {
    match T::deserialize(&record) {
        Ok(row) => Ok(row.cells()),
        Err(_) => Err(record),
    }
}
