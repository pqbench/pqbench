//! `table`: one table's record, or the tables in a schema.

use clap::{Args, Subcommand};

use crate::CliError;

mod get;
mod ls;

/// Arguments for `table`: one table's record, or the tables in a schema.
#[derive(Args)]
pub(crate) struct TableArgs {
    #[command(subcommand)]
    command: TableCommand,
}

#[derive(Subcommand)]
pub(crate) enum TableCommand {
    /// Read one table's record
    Get(get::GetArgs),
    /// List the tables in a schema
    Ls(ls::LsArgs),
}

pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &TableArgs,
) -> Result<(), CliError> {
    match &args.command {
        TableCommand::Get(args) => get::run(workspace, lake, args).await,
        TableCommand::Ls(args) => ls::run(workspace, args).await,
    }
}
