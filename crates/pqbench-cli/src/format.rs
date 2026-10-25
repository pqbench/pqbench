//! `format`: render a record stream as tables.
//!
//! The walk is NDJSON end to end; this is the one command that turns a stream
//! back into something a human reads. Records are typed by their `name` — the
//! model's identity (AIP-122) — so a `catalogs/…/schemas/…` line is a `Schema`
//! and takes its columns from [`crate::render`]. A record the curated type
//! cannot hold, or whose name has no columns yet, still renders from its own
//! keys; a record with no name at all (a `diff` delta) is not table data.

use clap::Args;
use pqbench::v1;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::render::{self, Row};
use crate::CliError;

/// Arguments for `format`: none — standard input to standard output.
#[derive(Args)]
pub(crate) struct FormatArgs {}

/// What a record is, for grouping.
#[derive(Clone, PartialEq, Eq)]
enum Class {
    Metastore,
    Catalog,
    Schema,
    Table,
    Partition,
    TableFile,
    Bytemass,
    Sweep,
    CompressionColumn,
    Profile,
    /// A record with no curated columns, grouped by its key set.
    Other(String),
}

pub(crate) async fn run(_args: &FormatArgs) -> Result<(), CliError> {
    let mut sections: Vec<(Class, Section)> = Vec::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(&line)
            .map_err(|error| format!("format reads NDJSON records: {error}"))?;
        let Some(class) = classify(&record) else {
            continue;
        };
        if let Some(record) = section(&mut sections, class).push(record) {
            // The curated type did not fit the record; its keys still render.
            let fallback = Class::Other(signature(&record));
            section(&mut sections, fallback).push_raw(record);
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

/// A record's class: the resource its `name` names, else the row kind.
fn classify(record: &Value) -> Option<Class> {
    let name = record.get("name")?.as_str()?;
    Some(if v1::name::parse_bytemass(name).is_some() {
        Class::Bytemass
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

/// One table's rows: a curated type, or JSON records with no columns yet.
enum Section {
    Metastore(Vec<v1::Metastore>),
    Catalog(Vec<v1::Catalog>),
    Schema(Vec<v1::Schema>),
    Table(Vec<v1::Table>),
    Partition(Vec<v1::Partition>),
    TableFile(Vec<v1::TableFile>),
    Bytemass(Vec<v1::Bytemass>),
    Sweep(Vec<render::SweepRow>),
    CompressionColumn(Vec<render::ColumnSweepRow>),
    Profile(Vec<render::ProfileRow>),
    Other(Vec<Value>),
}

impl Section {
    fn new(class: &Class) -> Self {
        match class {
            Class::Metastore => Self::Metastore(Vec::new()),
            Class::Catalog => Self::Catalog(Vec::new()),
            Class::Schema => Self::Schema(Vec::new()),
            Class::Table => Self::Table(Vec::new()),
            Class::Partition => Self::Partition(Vec::new()),
            Class::TableFile => Self::TableFile(Vec::new()),
            Class::Bytemass => Self::Bytemass(Vec::new()),
            Class::Sweep => Self::Sweep(Vec::new()),
            Class::CompressionColumn => Self::CompressionColumn(Vec::new()),
            Class::Profile => Self::Profile(Vec::new()),
            Class::Other(_) => Self::Other(Vec::new()),
        }
    }

    /// Take the record, or give it back when the curated type cannot hold it.
    fn push(&mut self, record: Value) -> Option<Value> {
        match self {
            Self::Metastore(rows) => push_typed(rows, record),
            Self::Catalog(rows) => push_typed(rows, record),
            Self::Schema(rows) => push_typed(rows, record),
            Self::Table(rows) => push_typed(rows, record),
            Self::Partition(rows) => push_typed(rows, record),
            Self::TableFile(rows) => push_typed(rows, record),
            Self::Bytemass(rows) => push_typed(rows, record),
            Self::Sweep(rows) => push_typed(rows, record),
            Self::CompressionColumn(rows) => push_typed(rows, record),
            Self::Profile(rows) => push_typed(rows, record),
            Self::Other(rows) => {
                rows.push(record);
                None
            }
        }
    }

    fn push_raw(&mut self, record: Value) {
        if let Self::Other(rows) = self {
            rows.push(record);
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Metastore(rows) => render::render(rows),
            Self::Catalog(rows) => render::render(rows),
            Self::Schema(rows) => render::render(rows),
            Self::Table(rows) => render::render(rows),
            Self::Partition(rows) => render::render(rows),
            Self::TableFile(rows) => render::render(rows),
            Self::Bytemass(rows) => render::render(rows),
            Self::Sweep(rows) => render::render(rows),
            Self::CompressionColumn(rows) => render::render(rows),
            Self::Profile(rows) => render::render(rows),
            Self::Other(rows) => render::render_values(rows),
        }
    }
}

/// Deserialize into the curated type; a record the type cannot hold comes back
/// for the generic table, so nothing is dropped silently.
fn push_typed<T: Row + serde::de::DeserializeOwned>(
    rows: &mut Vec<T>,
    record: Value,
) -> Option<Value> {
    match serde_json::from_value(record.clone()) {
        Ok(row) => {
            rows.push(row);
            None
        }
        Err(_) => Some(record),
    }
}
