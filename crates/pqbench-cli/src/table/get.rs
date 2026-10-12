//! `table get`: one table's record, read from its catalog and storage.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::table::get;

use crate::emit::{Emitter, Format};
use crate::source::{self, read_input, ref_env, ref_storage, split_table, table_ref};
use crate::CliError;

/// The default tables in flight for `table get`: one per core.
fn default_fan_out() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Arguments for `table get`: the `catalog.schema.table` name and the output
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

/// Read one table's record with the lease the stream carries: the lake
/// source's options and the ref's own. The record keeps the lease as its
/// `credentials`, so a later stage reads the table's files under the same
/// lease.
pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &GetArgs,
) -> Result<(), CliError> {
    let input = read_input("table get").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(table) = &args.table {
        if input.first.is_some() {
            return Err("table get takes CATALOG.SCHEMA.TABLE or a table stream, not both".into());
        }
        let (catalog, schema, name) = split_table("table get", table)?;
        let storage = get::Storage {
            location: None,
            env: BTreeMap::new(),
        };
        let record = get::read(workspace, lake, &catalog, &schema, &name, &storage).await?;
        emit.write_row(&record).await?;
        return emit.finish("tables: 1\n").await;
    }
    if !input.piped {
        return Err("table get needs CATALOG.SCHEMA.TABLE or a table stream".into());
    }
    let records = source::records("table get", input.first, input.lines);
    let mut tables = 0;
    let mut reads = records
        .map(|record| async {
            let record = record?;
            let (catalog, schema, name) = table_ref("table get", &record)?;
            let storage = get::Storage {
                location: ref_storage(&record).map(str::to_owned),
                env: ref_env(&record),
            };
            Ok::<_, CliError>(get::read(workspace, lake, &catalog, &schema, &name, &storage).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(record) = reads.next().await {
        emit.write_row(&record?).await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await
}
