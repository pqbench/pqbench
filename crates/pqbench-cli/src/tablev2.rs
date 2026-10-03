use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::StreamExt;
use pqbench::table::{Column, TableFormat, TableInfo};
use pqbench::tablev2::info;
use serde::Serialize;
use serde_json::Value;

use crate::emit::{Align, Emitter, Format, Row};
use crate::source::{self, read_input};
use crate::CliError;

/// Arguments for `tablev2`: one table's record.
#[derive(Args)]
pub(crate) struct TableV2Args {
    #[command(subcommand)]
    command: TableV2Command,
}

#[derive(Subcommand)]
pub(crate) enum TableV2Command {
    /// Show one table's record
    Info(InfoArgs),
}

/// Arguments for `tablev2 info`: the `catalog.schema.table` name and the
/// output flags.
#[derive(Args)]
pub(crate) struct InfoArgs {
    /// table name (`catalog.schema.table`); without it, read `pqbench.table-ref` v2 refs on stdin
    table: Option<String>,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(args: &TableV2Args) -> Result<(), CliError> {
    match &args.command {
        TableV2Command::Info(args) => run_info(args).await,
    }
}

async fn run_info(args: &InfoArgs) -> Result<(), CliError> {
    let input = read_input("tablev2 info").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(table) = &args.table {
        if input.first.is_some() {
            return Err(
                "tablev2 info takes CATALOG.SCHEMA.TABLE or a pqbench.table-ref v2 stream, not both"
                    .into(),
            );
        }
        let (catalog, schema, name) = split_table("tablev2 info", table)?;
        let record = info::read(
            &input.source.endpoint,
            &catalog,
            &schema,
            &name,
            input.source.token.as_deref(),
            input.source.table_format.into(),
            &input.source.env,
        )
        .await?;
        emit.write_row(&table_record(&record)).await?;
        return emit.finish("tables: 1\n").await;
    }
    if !input.piped {
        return Err(
            "tablev2 info needs CATALOG.SCHEMA.TABLE or a pqbench.table-ref v2 stream".into(),
        );
    }
    let records = source::records("tablev2 info", input.first, input.lines);
    let source = input.source;
    let mut tables = 0;
    let mut reads = records
        .map(|record| async {
            let (catalog, schema, name) = table_ref(&record?)?;
            Ok::<_, CliError>(
                info::read(
                    &source.endpoint,
                    &catalog,
                    &schema,
                    &name,
                    source.token.as_deref(),
                    source.table_format.into(),
                    &source.env,
                )
                .await?,
            )
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(record) = reads.next().await {
        emit.write_row(&table_record(&record?)).await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await
}

/// The catalog, schema, and table a `pqbench.table-ref` v2 ref names.
///
/// Version 1 refs (the legacy `lake` stream) are rejected, so the old and new
/// trees never consume each other.
fn table_ref(record: &Value) -> Result<(String, String, String), CliError> {
    let kind = record["kind"].as_str().unwrap_or_default();
    if kind != "pqbench.table-ref" {
        return Err(format!("expected pqbench.table-ref records, found {kind:?}").into());
    }
    if record["version"].as_u64() != Some(2) {
        return Err("tablev2 info reads pqbench.table-ref version 2; run `schema ls` first".into());
    }
    let id = record["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or("a pqbench.table-ref record needs an id")?;
    split_table("tablev2 info", id)
}

/// Split `catalog.schema.table` at the first and last dots; an Iceberg
/// namespace keeps its remaining dots.
fn split_table(command: &str, fqn: &str) -> Result<(String, String, String), CliError> {
    let Some((catalog, rest)) = fqn.split_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    };
    let Some((schema, table)) = rest.rsplit_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    };
    if catalog.is_empty() || schema.is_empty() || table.is_empty() {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    }
    Ok((catalog.to_string(), schema.to_string(), table.to_string()))
}

/// The document `tablev2 info` writes: the `schema ls` ref enriched with the
/// table's record.
#[derive(Serialize)]
struct TableRefRecord<'a> {
    kind: &'static str,
    version: u32,
    id: &'a str,
    format: TableFormat,
    storage_path: &'a str,
    snapshot_version: u64,
    #[serde(skip_serializing_if = "is_empty_slice")]
    partition_columns: &'a [String],
    #[serde(skip_serializing_if = "is_empty_slice")]
    columns: &'a [Column],
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    delta_properties: &'a BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    iceberg_properties: &'a BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    env: &'a BTreeMap<String, String>,
}

fn is_empty_slice<T>(values: &&[T]) -> bool {
    values.is_empty()
}

/// The row `tablev2 info` writes for a table.
fn table_record(record: &TableInfo) -> TableRefRecord<'_> {
    TableRefRecord {
        kind: "pqbench.table-ref",
        version: 2,
        id: &record.name,
        format: record.format,
        storage_path: &record.uri,
        snapshot_version: record.snapshot_version,
        partition_columns: record.partition_columns.as_slice(),
        columns: record.columns.as_slice(),
        delta_properties: &record.delta_properties,
        iceberg_properties: &record.iceberg_properties,
        env: &record.env,
    }
}

impl Row for TableRefRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "format", "snapshot", "columns", "location"];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Left,
    ];

    fn cells(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            format_name(self.format).to_string(),
            self.snapshot_version.to_string(),
            self.columns.len().to_string(),
            self.storage_path.to_string(),
        ]
    }
}

/// The table format as the document spells it.
fn format_name(format: TableFormat) -> &'static str {
    match format {
        TableFormat::DELTA => "delta",
        TableFormat::ICEBERG => "iceberg",
        _ => "unspecified",
    }
}
