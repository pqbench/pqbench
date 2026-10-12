//! `catalog`: one catalog's record, or the catalogs at the endpoint.

use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::emit::Format;
use crate::CliError;

pub(crate) mod get;
pub(crate) mod ls;

/// Arguments for `catalog`: one catalog's record and the catalogs at the
/// endpoint.
#[derive(Args)]
pub(crate) struct CatalogArgs {
    #[command(subcommand)]
    command: CatalogCommand,
}

#[derive(Subcommand)]
pub(crate) enum CatalogCommand {
    /// Show one catalog's record
    Get(NameArgs),
    /// List the catalogs at the endpoint
    Ls(ls::LsArgs),
}

/// Arguments for `catalog get`: the catalog name and the output flags.
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
        CatalogCommand::Get(args) => get::run(workspace, args).await,
        CatalogCommand::Ls(args) => ls::run(workspace, args).await,
    }
}
