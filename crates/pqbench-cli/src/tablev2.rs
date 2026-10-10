use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::StreamExt;
use pqbench::table::{Column, TableFormat, TableInfo};
use pqbench::tablev2::info;
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Resolved, Row};
use crate::source::{self, read_input, ref_env, split_table, table_ref};
use crate::CliError;

/// The default tables in flight for the per-table stages: one per core.
fn default_fan_out() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

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
    /// tables in flight at once (default: cores)
    #[arg(long, default_value_t = default_fan_out())]
    fan_out: usize,
}

/// The resolved configuration of `tablev2 info`: the table name (or stdin) and
/// the output flags, from the command line.
pub(crate) struct TableV2InfoConfig {
    table: Option<String>,
    format: Resolved,
    output: Option<PathBuf>,
    fan_out: usize,
}

impl TableV2InfoConfig {
    fn resolve(args: &InfoArgs) -> Self {
        Self {
            table: args.table.clone(),
            format: args.format.resolve(false),
            output: args.output.clone(),
            fan_out: args.fan_out.max(1),
        }
    }
}

pub(crate) async fn run(args: &TableV2Args) -> Result<(), CliError> {
    match &args.command {
        TableV2Command::Info(args) => run_info(&TableV2InfoConfig::resolve(args)).await,
    }
}

/// Read one table's record with the env the stream carries: the lake source's
/// options, the ref's own, and the process environment the storage client also
/// reads. Credentials are the `credentials` stage's concern — `credentials get`
/// materializes them onto refs; this read consumes them and passes them on, so
/// a later stage reads the table's files under the same lease.
async fn run_info(config: &TableV2InfoConfig) -> Result<(), CliError> {
    let context = read_input("tablev2 info").await?;
    let mut emit = Emitter::open(config.output.as_deref(), config.format)?;
    if let Some(table) = &config.table {
        if context.first.is_some() {
            return Err(
                "tablev2 info takes CATALOG.SCHEMA.TABLE or a pqbench.table-ref v2 stream, not both"
                    .into(),
            );
        }
        let (catalog, schema, name) = split_table("tablev2 info", table)?;
        let storage = info::Storage {
            location: None,
            env: context.source.env.clone(),
        };
        let record = read_record(&context.source, &catalog, &schema, &name, &storage).await?;
        emit.write_row(&table_record(&record, storage.env)).await?;
        return emit.finish("tables: 1\n").await;
    }
    if !context.piped {
        return Err(
            "tablev2 info needs CATALOG.SCHEMA.TABLE or a pqbench.table-ref v2 stream".into(),
        );
    }
    let records = source::records("tablev2 info", context.first, context.lines);
    let source = context.source;
    let mut tables = 0;
    let mut reads = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("tablev2 info", &record)?;
            let storage = info::Storage {
                location: record["storage_path"].as_str().map(str::to_owned),
                env: ref_env(&record, &source.env),
            };
            let record = read_record(&source, &catalog, &schema, &name, &storage).await?;
            Ok::<_, CliError>((record, storage.env))
        })
        .buffer_unordered(config.fan_out);
    while let Some(record) = reads.next().await {
        let (record, env) = record?;
        emit.write_row(&table_record(&record, env)).await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await
}

/// The table's record, read with `env`: the caller supplies the env the read
/// runs under (the lake source's, merged with the ref's).
async fn read_record(
    source: &source::Source,
    catalog: &str,
    schema: &str,
    name: &str,
    storage: &info::Storage,
) -> Result<TableInfo, CliError> {
    Ok(info::read(
        &source.endpoint,
        catalog,
        schema,
        name,
        source.token.as_deref(),
        source.table_format.into(),
        storage,
    )
    .await?)
}

/// The document `tablev2 info` writes: the `schema ls` ref enriched with the
/// table's record and the env the read ran under, so a later stage
/// (`bytemass`) reads the table's files under the same lease.
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
    env: BTreeMap<String, String>,
}

fn is_empty_slice<T>(values: &&[T]) -> bool {
    values.is_empty()
}

/// The row `tablev2 info` writes for a table: the record plus the env the read
/// ran under.
fn table_record(info: &TableInfo, env: BTreeMap<String, String>) -> TableRefRecord<'_> {
    TableRefRecord {
        kind: "pqbench.table-ref",
        version: 2,
        id: &info.name,
        format: info.format,
        storage_path: &info.uri,
        snapshot_version: info.snapshot_version,
        partition_columns: info.partition_columns.as_slice(),
        columns: info.columns.as_slice(),
        delta_properties: &info.delta_properties,
        iceberg_properties: &info.iceberg_properties,
        env,
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
