//! `schema`: one schema's record, or the schemas in a catalog.

use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::emit::Format;
use crate::CliError;

pub(crate) mod get;
pub(crate) mod ls;

/// Arguments for `schema`: one schema's record and the schemas in a catalog.
#[derive(Args)]
pub(crate) struct SchemaArgs {
    #[command(subcommand)]
    command: SchemaCommand,
}

#[derive(Subcommand)]
pub(crate) enum SchemaCommand {
    /// Show one schema's record
    Get(NameArgs),
    /// List the schemas in a catalog
    Ls(ls::LsArgs),
}

/// Arguments for `schema get`: the `catalog.schema` name and the output flags.
#[derive(Args)]
pub(crate) struct NameArgs {
    /// schema name (`catalog.schema`); without it, read schema refs on stdin
    schema: Option<String>,
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

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &SchemaArgs) -> Result<(), CliError> {
    match &args.command {
        SchemaCommand::Get(args) => get::run(workspace, args).await,
        SchemaCommand::Ls(args) => ls::run(workspace, args).await,
    }
}
