//! `list tables`: the tables in a schema, one `Table` line each.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::table::ls;

use crate::emit::{Emitter, Format};
use crate::source::{self, read_input, schema_ref, split_schema};
use crate::CliError;

/// Arguments for `list tables`.
#[derive(Args)]
pub(crate) struct LsArgs {
    /// schema name (`catalog.schema`); without it, read schema refs on stdin
    schema: Option<String>,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// schemas in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

/// List a schema's tables: one `Table` line each, the record `get table`
/// enriches.
pub(crate) async fn run(workspace: &pqbench::Workspace, args: &LsArgs) -> Result<(), CliError> {
    let input = read_input("list tables").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err("list tables takes CATALOG.SCHEMA or a schema stream, not both".into());
        }
        let (catalog, name) = split_schema("list tables", schema)?;
        let tables = ls::list(workspace, &catalog, &name).await?;
        for table in &tables {
            emit.write_row(table).await?;
        }
        return emit.finish(&format!("tables: {}\n", tables.len())).await;
    }
    if !input.piped {
        return Err("list tables needs CATALOG.SCHEMA or a schema stream".into());
    }
    let records = source::records("list tables", input.first, input.lines);
    let mut tables = 0;
    let mut lists = records
        .map(|record| async {
            let (catalog, name) = schema_ref("list tables", &record?)?;
            Ok::<_, CliError>(ls::list(workspace, &catalog, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(listed) = lists.next().await {
        for table in listed? {
            emit.write_row(&table).await?;
            tables += 1;
        }
    }
    emit.finish(&format!("tables: {tables}\n")).await
}
