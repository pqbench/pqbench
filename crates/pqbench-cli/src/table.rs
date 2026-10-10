use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::StreamExt;
use pqbench::table::{self, info, Column, TableFormat, TableInfo};
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Resolved, Row};
use crate::source::{self, read_input, read_storage_input, ref_env, split_table, table_ref};
use crate::CliError;

/// The default tables in flight for `table info`: one per core.
fn default_fan_out() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Arguments for `table`: one table's record, or its natural partitions.
#[derive(Args)]
pub(crate) struct TableArgs {
    #[command(subcommand)]
    command: TableCommand,
}

#[derive(Subcommand)]
pub(crate) enum TableCommand {
    /// Read one table's record
    Info(InfoArgs),
    /// List the table's natural partitions, grouped by commit time
    Ls(LsArgs),
}

pub(crate) async fn run(args: &TableArgs) -> Result<(), CliError> {
    match &args.command {
        TableCommand::Info(info) => run_info(&InfoConfig::resolve(info)).await,
        TableCommand::Ls(ls) => run_ls(ls).await,
    }
}

/// Arguments for `table info`: the `catalog.schema.table` name and the output
/// flags.
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

/// The resolved configuration of `table info`: the table name (or stdin) and
/// the output flags, from the command line.
pub(crate) struct InfoConfig {
    table: Option<String>,
    format: Resolved,
    output: Option<PathBuf>,
    fan_out: usize,
}

impl InfoConfig {
    fn resolve(args: &InfoArgs) -> Self {
        Self {
            table: args.table.clone(),
            format: args.format.resolve(false),
            output: args.output.clone(),
            fan_out: args.fan_out.max(1),
        }
    }
}

/// Read one table's record with the env the stream carries: the lake source's
/// options, the ref's own, and the process environment the storage client also
/// reads. Credentials are the `credentials` stage's concern — `credentials get`
/// materializes them onto refs; this read consumes them and passes them on, so
/// a later stage reads the table's files under the same lease.
async fn run_info(config: &InfoConfig) -> Result<(), CliError> {
    let context = read_input("table info").await?;
    let mut emit = Emitter::open(config.output.as_deref(), config.format)?;
    if let Some(table) = &config.table {
        if context.first.is_some() {
            return Err(
                "table info takes CATALOG.SCHEMA.TABLE or a pqbench.table-ref v2 stream, not both"
                    .into(),
            );
        }
        let (catalog, schema, name) = split_table("table info", table)?;
        let storage = info::Storage {
            location: None,
            env: BTreeMap::new(),
        };
        let record = read_record(&context.source, &catalog, &schema, &name, &storage).await?;
        emit.write_row(&table_record(&record, storage.env)).await?;
        return emit.finish("tables: 1\n").await;
    }
    if !context.piped {
        return Err(
            "table info needs CATALOG.SCHEMA.TABLE or a pqbench.table-ref v2 stream".into(),
        );
    }
    let records = source::records("table info", context.first, context.lines);
    let source = context.source;
    let mut tables = 0;
    let mut reads = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("table info", &record)?;
            let storage = info::Storage {
                location: record["storage_path"].as_str().map(str::to_owned),
                env: ref_env(&record),
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

/// The document `table info` writes: the `schema ls` ref enriched with the
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

/// The row `table info` writes for a table: the record plus the env the read
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

/// Arguments for `table ls`.
#[derive(Args)]
pub(crate) struct LsArgs {
    /// table URI or a document file; without it, read table refs on stdin
    input: Option<String>,
    /// window width, epoch-aligned: e.g. 1h, 1d, 1w
    #[arg(long, default_value = "1d")]
    every: String,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// tables in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

/// List a table's natural partitions: commits grouped into commit-time windows.
async fn run_ls(args: &LsArgs) -> Result<(), CliError> {
    let window = parse_window(&args.every)?;
    let context = read_storage_input("table ls").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let mut partitions = 0usize;
    match &args.input {
        Some(uri) if uri != "-" => {
            if context.first.is_some() {
                return Err(
                    "table ls takes a table URI or a pqbench.table-ref v2 stream, not both".into(),
                );
            }
            let env = BTreeMap::new();
            partitions += list_partitions(uri, &env, window, &mut emit).await?;
        }
        _ => {
            if !context.piped {
                return Err("table ls needs a table URI or a pqbench.table-ref v2 stream".into());
            }
            let records = source::records("table ls", context.first, context.lines);
            let mut reads = records
                .map(|record| async {
                    let record = record?;
                    table_ref("table ls", &record)?;
                    let uri = record["storage_path"]
                        .as_str()
                        .filter(|path| !path.is_empty())
                        .ok_or_else(|| {
                            CliError::from(
                                "a table-ref has no storage path; run `pqbench table info` first",
                            )
                        })?
                        .to_string();
                    let env = ref_env(&record);
                    let found = table::ls::list(&uri, &env, window)
                        .await
                        .map_err(|error| CliError::from(error.to_string()))?;
                    Ok::<_, CliError>((uri, env, found))
                })
                .buffer_unordered(args.fan_out.max(1));
            while let Some(result) = reads.next().await {
                let (uri, env, found) = result?;
                for partition in &found {
                    emit.write_row(&partition_record(&uri, partition, &env))
                        .await?;
                    partitions += 1;
                }
            }
        }
    }
    emit.finish(&format!("partitions: {partitions}\n")).await
}

/// Emit one table's partitions; returns how many were written.
async fn list_partitions(
    uri: &str,
    env: &BTreeMap<String, String>,
    window: i64,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let found = table::ls::list(uri, env, window)
        .await
        .map_err(|error| CliError::from(error.to_string()))?;
    let mut count = 0;
    for partition in &found {
        emit.write_row(&partition_record(uri, partition, env))
            .await?;
        count += 1;
    }
    Ok(count)
}

/// Parse a window width: a positive integer and a unit (`m`, `h`, `d`, `w`).
fn parse_window(value: &str) -> Result<i64, CliError> {
    let (number, unit) = value.split_at(
        value
            .len()
            .checked_sub(1)
            .ok_or_else(|| CliError::from("--every is empty"))?,
    );
    let count: i64 = number
        .parse()
        .map_err(|_| CliError::from(format!("--every `{value}` needs a number and a unit")))?;
    if count <= 0 {
        return Err(format!("--every must be positive: {value}").into());
    }
    let millis = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return Err(format!("--every unit must be m, h, d, or w: {value}").into()),
    };
    Ok(count * millis)
}

/// The document `table ls` writes, one line per partition.
#[derive(Serialize)]
struct PartitionRecord<'a> {
    kind: &'static str,
    version: u32,
    table: &'a str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    env: &'a BTreeMap<String, String>,
    definition: Definition,
    commits: Vec<CommitCell>,
}

/// A partition's definition: a natural commit-time window.
#[derive(Serialize)]
struct Definition {
    kind: &'static str,
    first_time: i64,
    last_time: i64,
}

/// One commit in a partition, in the emitted document.
#[derive(Serialize)]
struct CommitCell {
    version: u64,
    commit_time: i64,
}

impl Row for PartitionRecord<'_> {
    const HEADER: &'static [&'static str] = &["table", "first_time", "last_time", "commits"];
    const ALIGN: &'static [Align] = &[Align::Left, Align::Right, Align::Right, Align::Right];

    fn cells(&self) -> Vec<String> {
        vec![
            self.table.to_string(),
            self.definition.first_time.to_string(),
            self.definition.last_time.to_string(),
            self.commits.len().to_string(),
        ]
    }
}

fn partition_record<'a>(
    table: &'a str,
    partition: &table::ls::Partition,
    env: &'a BTreeMap<String, String>,
) -> PartitionRecord<'a> {
    PartitionRecord {
        kind: "pqbench.partition",
        version: 1,
        table,
        env,
        definition: Definition {
            kind: "natural",
            first_time: partition.first_time,
            last_time: partition.last_time,
        },
        commits: partition
            .commits
            .iter()
            .map(|commit| CommitCell {
                version: commit.version,
                commit_time: commit.commit_time,
            })
            .collect(),
    }
}
