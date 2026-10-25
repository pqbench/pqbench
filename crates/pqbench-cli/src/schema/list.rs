//! `list schemas`: the schemas in a catalog, one `Schema` line each.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::schema::list;

use crate::emit::{Emitter, Format};
use crate::error::{fail_when_dropped, ReadOutcome, RecordError};
use crate::source::{self, catalog_ref, read_input, Input};
use crate::CliError;

/// Arguments for `list schemas`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// catalog name; without it, read `Catalog` refs on stdin
    catalog: Option<String>,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// catalogs in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &ListArgs) -> Result<(), CliError> {
    let input = read_input("list schemas").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let (schemas, dropped) = match &args.catalog {
        Some(catalog) => {
            if input.first.is_some() {
                return Err("list schemas takes CATALOG or a catalog stream, not both".into());
            }
            (list_catalog(workspace, catalog, &mut emit).await?, 0)
        }
        None => list_stream(workspace, input, args.fan_out, &mut emit).await?,
    };
    emit.finish(&format!("schemas: {schemas}\n")).await?;
    fail_when_dropped(&[("catalog", dropped)])
}

/// List one named catalog's schemas.
async fn list_catalog(
    workspace: &pqbench::Workspace,
    catalog: &str,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let schemas = list::list(workspace, &pqbench::v1::name::catalog(catalog)).await?;
    for schema in &schemas {
        emit.write_row(schema).await?;
    }
    Ok(schemas.len())
}

/// List every catalog's schemas from a catalog stream.
async fn list_stream(
    workspace: &pqbench::Workspace,
    input: Input,
    fan_out: usize,
    emit: &mut Emitter,
) -> Result<(usize, usize), CliError> {
    if !input.piped {
        return Err("list schemas needs CATALOG or a catalog stream".into());
    }
    let records = source::records("list schemas", input.first, input.lines);
    let mut schemas = 0;
    let mut dropped = 0;
    let mut lists = records
        .map(|record| async {
            let name = catalog_ref("list schemas", &record?)?;
            let outcome = match list::list(workspace, &name).await {
                Ok(listed) => ReadOutcome::Record(listed),
                Err(error) => ReadOutcome::Dropped(RecordError::new("catalog", &name, error)),
            };
            Ok::<_, CliError>(outcome)
        })
        .buffer_unordered(fan_out.max(1));
    while let Some(outcome) = lists.next().await {
        match outcome? {
            ReadOutcome::Record(listed) => {
                for schema in listed {
                    emit.write_row(&schema).await?;
                    schemas += 1;
                }
            }
            ReadOutcome::Dropped(error) => {
                error.report();
                dropped += 1;
            }
        }
    }
    Ok((schemas, dropped))
}
