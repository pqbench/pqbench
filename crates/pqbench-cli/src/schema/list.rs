//! `list schemas`: the schemas in a catalog, one `Schema` line each.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::schema::list;

use crate::emit::{Emitter, Format};
use crate::source::{self, catalog_ref, read_input};
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
    if let Some(catalog) = &args.catalog {
        if input.first.is_some() {
            return Err("list schemas takes CATALOG or a catalog stream, not both".into());
        }
        let schemas = list::list(workspace, &pqbench::v1::name::catalog(catalog)).await?;
        for schema in &schemas {
            emit.write_row(schema).await?;
        }
        return emit.finish(&format!("schemas: {}\n", schemas.len())).await;
    }
    if !input.piped {
        return Err("list schemas needs CATALOG or a catalog stream".into());
    }
    let records = source::records("list schemas", input.first, input.lines);
    let mut schemas = 0;
    let mut lists = records
        .map(|record| async {
            let name = catalog_ref("list schemas", &record?)?;
            Ok::<_, CliError>(list::list(workspace, &name).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(listed) = lists.next().await {
        for schema in listed? {
            emit.write_row(&schema).await?;
            schemas += 1;
        }
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}
