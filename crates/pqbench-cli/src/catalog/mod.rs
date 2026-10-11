//! `catalog`: one catalog's record, or the schemas in it.

use std::path::PathBuf;

use clap::{Args, Subcommand};
use serde_json::Value;

use crate::emit::Format;
use crate::CliError;

mod info;
mod ls;

/// Arguments for `catalog`: one catalog's record.
#[derive(Args)]
pub(crate) struct CatalogArgs {
    #[command(subcommand)]
    command: CatalogCommand,
}

#[derive(Subcommand)]
pub(crate) enum CatalogCommand {
    /// Show one catalog's record
    Info(NameArgs),
    /// List the schemas in a catalog
    Ls(NameArgs),
}

/// Arguments for a catalog subcommand: the catalog name and the output flags.
#[derive(Args)]
pub(crate) struct NameArgs {
    /// catalog name; without it, read `Catalog` refs on stdin
    catalog: Option<String>,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    args: &CatalogArgs,
) -> Result<(), CliError> {
    match &args.command {
        CatalogCommand::Info(args) => info::run(workspace, args).await,
        CatalogCommand::Ls(args) => ls::run(workspace, args).await,
    }
}

/// The catalog name a catalog ref names: a `catalogs/{catalog}` resource name.
fn catalog_name(record: &Value) -> Result<String, CliError> {
    record["name"]
        .as_str()
        .and_then(pqbench::v1::name::parse_catalog)
        .ok_or_else(|| "expected a catalog resource name (`catalogs/{catalog}`)".into())
}
