//! `credentials`: vended storage options for a table, and the readability check.

use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::emit::Format;
use crate::CliError;

mod check;
mod get;

/// Arguments for `credentials`: one table's storage options.
#[derive(Args)]
pub(crate) struct CredentialsArgs {
    #[command(subcommand)]
    command: CredentialsCommand,
}

#[derive(Subcommand)]
pub(crate) enum CredentialsCommand {
    /// Put vended read credentials on each table-ref
    Get(GetArgs),
    /// Check each table-ref is readable outside Databricks compute
    Check(CheckArgs),
}

/// Arguments for `credentials get`: the shared output flags.
#[derive(Args)]
pub(crate) struct GetArgs {
    #[command(flatten)]
    stage: StageArgs,
}

/// Arguments for `credentials check`: the shared output flags.
#[derive(Args)]
pub(crate) struct CheckArgs {
    #[command(flatten)]
    stage: StageArgs,
}

/// The output flags both credentials stages share.
#[derive(Args)]
pub(crate) struct StageArgs {
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
    args: &CredentialsArgs,
) -> Result<(), CliError> {
    match &args.command {
        CredentialsCommand::Get(args) => get::run(workspace, args).await,
        CredentialsCommand::Check(args) => check::run(workspace, args).await,
    }
}
