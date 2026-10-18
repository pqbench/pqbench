//! `get table`: one table's record, read from its catalog and storage.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::table::get;

use crate::emit::{Emitter, Format};
use crate::source::{self, read_input, split_table, table_ref};
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

/// Read one table's record from the ref the stream carries: the catalog names
/// the table and its location, the ref names the lease the storage read uses
/// and, when it has one, the path that read can start on. The record keeps the
/// lease as its `credentials`, so a later stage reads the table's files under
/// the same lease.
pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &GetArgs,
) -> Result<(), CliError> {
    let input = read_input("get table").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(table) = &args.table {
        if input.first.is_some() {
            return Err("get table takes CATALOG.SCHEMA.TABLE or a table stream, not both".into());
        }
        let (catalog, schema, table) = split_table("get table", table)?;
        let table = pqbench::v1::Table {
            name: pqbench::v1::name::table(&catalog, &schema, &table),
            ..pqbench::v1::Table::default()
        };
        let record = get::get(workspace, lake, &table).await?;
        emit.write_row(&record).await?;
        return emit.finish("tables: 1\n").await;
    }
    if !input.piped {
        return Err("get table needs CATALOG.SCHEMA.TABLE or a table stream".into());
    }
    let records = source::records("get table", input.first, input.lines);
    let mut tables = 0;
    let mut reads = records
        .map(|record| async {
            let record = record?;
            table_ref("get table", &record)?;
            let table: pqbench::v1::Table = serde_json::from_value(record)
                .map_err(|error| CliError::from(error.to_string()))?;
            Ok::<_, CliError>(get::get(workspace, lake, &table).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(record) = reads.next().await {
        emit.write_row(&record?).await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await
}
