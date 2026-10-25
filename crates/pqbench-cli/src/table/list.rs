//! `list tables`: the tables in a schema, one `Table` line each.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::table::list;

use crate::emit::{Emitter, Format};
use crate::error::{fail_when_dropped, tolerate};
use crate::source::{self, read_input, schema_ref, split_schema, Input};
use crate::CliError;

/// Arguments for `list tables`.
#[derive(Args)]
pub(crate) struct ListArgs {
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
pub(crate) async fn run(workspace: &pqbench::Workspace, args: &ListArgs) -> Result<(), CliError> {
    let input = read_input("list tables").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let (tables, dropped) = match &args.schema {
        Some(schema) => {
            if input.first.is_some() {
                return Err("list tables takes CATALOG.SCHEMA or a schema stream, not both".into());
            }
            (list_schema(workspace, schema, &mut emit).await?, 0)
        }
        None => list_stream(workspace, input, args.fan_out, &mut emit).await?,
    };
    emit.finish(&format!("tables: {tables}\n")).await?;
    fail_when_dropped("schema", dropped)
}

/// List one named schema's tables.
async fn list_schema(
    workspace: &pqbench::Workspace,
    schema: &str,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let (catalog, name) = split_schema("list tables", schema)?;
    let tables = list::list(workspace, &pqbench::v1::name::schema(&catalog, &name)).await?;
    for table in &tables {
        emit.write_row(table).await?;
    }
    Ok(tables.len())
}

/// List every schema's tables from a schema stream.
async fn list_stream(
    workspace: &pqbench::Workspace,
    input: Input,
    fan_out: usize,
    emit: &mut Emitter,
) -> Result<(usize, usize), CliError> {
    if !input.piped {
        return Err("list tables needs CATALOG.SCHEMA or a schema stream".into());
    }
    let records = source::records("list tables", input.first, input.lines);
    let mut tables = 0;
    let mut dropped = 0;
    let mut lists = records
        .map(|record| async {
            let name = schema_ref("list tables", &record?)?;
            let listed = list::list(workspace, &name).await.map_err(CliError::from);
            Ok::<_, CliError>(tolerate(&name, listed))
        })
        .buffer_unordered(fan_out.max(1));
    while let Some(listed) = lists.next().await {
        let Some(listed) = listed? else {
            dropped += 1;
            continue;
        };
        for table in listed {
            emit.write_row(&table).await?;
            tables += 1;
        }
    }
    Ok((tables, dropped))
}
