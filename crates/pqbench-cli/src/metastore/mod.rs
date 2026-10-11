//! `metastore`: the endpoint's metastore record and its catalogs.

use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::emit::Format;
use crate::CliError;

mod info;
mod ls;

/// Arguments for `metastore`: the endpoint's metastore and its catalogs.
#[derive(Args)]
pub(crate) struct MetastoreArgs {
    #[command(subcommand)]
    command: MetastoreCommand,
}

#[derive(Subcommand)]
pub(crate) enum MetastoreCommand {
    /// Show the endpoint's metastore record
    Info(OutputArgs),
    /// List the catalogs at the endpoint
    Ls(OutputArgs),
}

#[derive(Args)]
pub(crate) struct OutputArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    args: &MetastoreArgs,
) -> Result<(), CliError> {
    match &args.command {
        MetastoreCommand::Info(args) => info::run(workspace, args).await,
        MetastoreCommand::Ls(args) => ls::run(workspace, args).await,
    }
}
