//! `partition`: a table's natural partitions.

use clap::{Args, Subcommand};

use crate::CliError;

pub(crate) mod ls;

/// Arguments for `partition`: a table's natural partitions.
#[derive(Args)]
pub(crate) struct PartitionArgs {
    #[command(subcommand)]
    command: PartitionCommand,
}

#[derive(Subcommand)]
pub(crate) enum PartitionCommand {
    /// List a table's natural partitions, grouped by commit time
    Ls(ls::LsArgs),
}

pub(crate) async fn run(lake: &pqbench::Lake, args: &PartitionArgs) -> Result<(), CliError> {
    match &args.command {
        PartitionCommand::Ls(ls) => ls::run(lake, ls).await,
    }
}
