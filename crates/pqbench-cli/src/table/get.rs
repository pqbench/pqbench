//! `get table`: one table's record, read from its catalog and storage.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::table::get;

use crate::emit::{Emitter, Format};
use crate::error::{fail_when_dropped, tolerate};
use crate::source::{
    self, dotted_name, read_input, ref_credentials, ref_storage, split_table, table_ref, Input,
};
use crate::CliError;

/// The default tables in flight for `get table`: one per core.
fn default_fan_out() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Arguments for `get table`: the `catalog.schema.table` name and the output
/// flags.
#[derive(Args)]
pub(crate) struct GetArgs {
    /// table name (`catalog.schema.table`); without it, read table refs on stdin
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

/// Read one table's record with the lease the stream carries: the ref names the
/// lease the storage read uses and, when it has one, the path that read can
/// start on; the catalog names the table and its location. The record keeps the
/// lease as its `credentials`, so a later stage reads the table's files under
/// the same lease.
pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &GetArgs,
) -> Result<(), CliError> {
    let input = read_input("get table").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let (tables, dropped) = match &args.table {
        Some(table) => {
            if input.first.is_some() {
                return Err(
                    "get table takes CATALOG.SCHEMA.TABLE or a table stream, not both".into(),
                );
            }
            (get_one(workspace, lake, table, &mut emit).await?, 0)
        }
        None => get_stream(workspace, lake, input, args.fan_out, &mut emit).await?,
    };
    emit.finish(&format!("tables: {tables}\n")).await?;
    fail_when_dropped("table", dropped)
}

/// Read one named table's record.
async fn get_one(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    table: &str,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let (catalog, schema, table) = split_table("get table", table)?;
    let name = pqbench::v1::name::table(&catalog, &schema, &table);
    let record = get::get(workspace, lake, &name, None, &BTreeMap::new()).await?;
    emit.write_row(&record).await?;
    Ok(1)
}

/// Read each table's record from a table stream.
async fn get_stream(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    input: Input,
    fan_out: usize,
    emit: &mut Emitter,
) -> Result<(usize, usize), CliError> {
    if !input.piped {
        return Err("get table needs CATALOG.SCHEMA.TABLE or a table stream".into());
    }
    let records = source::records("get table", input.first, input.lines);
    let mut tables = 0;
    let mut dropped = 0;
    let mut reads = records
        .map(|record| async {
            let record = record?;
            let name = table_ref("get table", &record)?;
            let uri = ref_storage(&record);
            let lease = ref_credentials(&record).filter(|lease| !lease.is_empty());
            let env = lease
                .as_ref()
                .map(pqbench::v1::Credentials::to_env)
                .unwrap_or_default();
            let read = get::get(workspace, lake, &name, uri, &env)
                .await
                .map_err(CliError::from)
                .map(|mut table| {
                    table.credentials = lease;
                    table
                });
            Ok::<_, CliError>(tolerate(&dotted_name(&name), read))
        })
        .buffer_unordered(fan_out.max(1));
    while let Some(record) = reads.next().await {
        let Some(record) = record? else {
            dropped += 1;
            continue;
        };
        emit.write_row(&record).await?;
        tables += 1;
    }
    Ok((tables, dropped))
}
