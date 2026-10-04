use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::StreamExt;
use pqbench::table::{Column, TableFormat, TableInfo};
use pqbench::tablev2::info;
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
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
        let env = vend_if_needed(
            &input.source,
            &catalog,
            &schema,
            &name,
            input.source.env.clone(),
        )
        .await?;
        let record = info::read(
            &input.source.endpoint,
            &catalog,
            &schema,
            &name,
            input.source.token.as_deref(),
            input.source.table_format.into(),
            &env,
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
            let record = record?;
            let (catalog, schema, name) = table_ref("tablev2 info", &record)?;
            let env = vend_if_needed(
                &source,
                &catalog,
                &schema,
                &name,
                ref_env(&record, &source.env),
            )
            .await?;
            Ok::<_, CliError>(
                info::read(
                    &source.endpoint,
                    &catalog,
                    &schema,
                    &name,
                    source.token.as_deref(),
                    source.table_format.into(),
                    &env,
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

/// The table's env, plus vended credentials when it names none: the credentials
/// stay in memory, and the emitted record never carries them. Only Unity vends
/// here — the Iceberg read carries its metadata inline.
async fn vend_if_needed(
    source: &source::Source,
    catalog: &str,
    schema: &str,
    name: &str,
    mut env: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, CliError> {
    if !matches!(source.table_format, source::TableFormat::Unity) || !needs_credentials(&env) {
        return Ok(env);
    }
    if let Some(credentials) = source::vend(source, catalog, schema, name).await? {
        env.extend(credentials);
    }
    Ok(env)
}

/// Whether the env names no credential: a public bucket or a set of keys skips
/// vending.
fn needs_credentials(env: &BTreeMap<String, String>) -> bool {
    !env.contains_key("AWS_ACCESS_KEY_ID")
        && env.get("AWS_SKIP_SIGNATURE").map(String::as_str) != Some("true")
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
