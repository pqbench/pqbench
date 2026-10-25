//! `credentials`: vended storage options for a table, and the readability check.

use clap::{Args, Subcommand};

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
    Get(StageArgs),
    /// Check each table-ref is readable outside Databricks compute
    Check(StageArgs),
}

/// The output flags both credentials stages share.
#[derive(Args)]
pub(crate) struct StageArgs {
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
