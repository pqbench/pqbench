//! `partition`: a partition's files.

use clap::{Args, Subcommand};

use crate::CliError;

mod ls;

/// Arguments for `partition`: a partition's files.
#[derive(Args)]
pub(crate) struct PartitionArgs {
    #[command(subcommand)]
    command: PartitionCommand,
}

#[derive(Subcommand)]
pub(crate) enum PartitionCommand {
    /// List a partition's files — the ones its commits added
    Ls(ls::LsArgs),
}

pub(crate) async fn run(lake: &pqbench::Lake, args: &PartitionArgs) -> Result<(), CliError> {
    match &args.command {
        PartitionCommand::Ls(ls) => ls::run(lake, ls).await,
    }
}
