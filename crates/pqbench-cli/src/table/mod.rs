//! `table`: one table's record, or its natural partitions.

use clap::{Args, Subcommand};

use crate::CliError;

mod info;
mod ls;

/// Arguments for `table`: one table's record, or its natural partitions.
#[derive(Args)]
pub(crate) struct TableArgs {
    #[command(subcommand)]
    command: TableCommand,
}

#[derive(Subcommand)]
pub(crate) enum TableCommand {
    /// Read one table's record
    Info(info::InfoArgs),
    /// List the table's natural partitions, grouped by commit time
    Ls(ls::LsArgs),
}

pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &TableArgs,
) -> Result<(), CliError> {
    match &args.command {
        TableCommand::Info(info) => info::run_info(workspace, lake, info).await,
        TableCommand::Ls(ls) => ls::run_ls(lake, ls).await,
    }
}

impl TableArgs {
    /// Whether the table subcommand needs `PQB_ENDPOINT`: `table info` reads a
    /// catalog; `table ls` reads only storage.
    pub(crate) fn requires_endpoint(&self) -> bool {
        matches!(self.command, TableCommand::Info(_))
    }
}
