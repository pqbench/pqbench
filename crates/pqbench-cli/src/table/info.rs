//! `table info`: one table's record, read from its catalog and storage.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::table::{info, Column, TableFormat, TableInfo};
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
use crate::source::{self, read_input, ref_env, split_table, table_ref};
use crate::CliError;

/// The default tables in flight for `table info`: one per core.
fn default_fan_out() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
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
    /// tables in flight at once (default: cores) [env: PQB_TABLE_FAN_OUT]
    #[arg(long, default_value_t = default_fan_out(), env = "PQB_TABLE_FAN_OUT")]
    fan_out: usize,
}

/// Read one table's record with the env the stream carries: the lake source's
/// options, the ref's own, and the process environment the storage client also
/// reads. Credentials are the `credentials` stage's concern — `credentials get`
/// materializes them onto refs; this read consumes them and passes them on, so
/// a later stage reads the table's files under the same lease.
pub(super) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &InfoArgs,
) -> Result<(), CliError> {
    let input = read_input("table info").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(table) = &args.table {
        if input.first.is_some() {
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
        let record = read_record(workspace, lake, &catalog, &schema, &name, &storage).await?;
        emit.write_row(&table_record(&record, storage.env)).await?;
        return emit.finish("tables: 1\n").await;
    }
    if !input.piped {
        return Err(
            "table info needs CATALOG.SCHEMA.TABLE or a pqbench.table-ref v2 stream".into(),
        );
    }
    let records = source::records("table info", input.first, input.lines);
    let mut tables = 0;
    let mut reads = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("table info", &record)?;
            let storage = info::Storage {
                location: record["storage_path"].as_str().map(str::to_owned),
                env: ref_env(&record),
            };
            let record = read_record(workspace, lake, &catalog, &schema, &name, &storage).await?;
            Ok::<_, CliError>((record, storage.env))
        })
        .buffer_unordered(args.fan_out.max(1));
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
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    catalog: &str,
    schema: &str,
    name: &str,
    storage: &info::Storage,
) -> Result<TableInfo, CliError> {
    Ok(info::read(workspace, lake, catalog, schema, name, storage).await?)
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
