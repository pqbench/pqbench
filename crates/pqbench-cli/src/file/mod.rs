//! `file`: a partition's files.

use clap::{Args, Subcommand};

use crate::CliError;

pub(crate) mod ls;

/// Arguments for `file`: a partition's files.
#[derive(Args)]
pub(crate) struct FileArgs {
    #[command(subcommand)]
    command: FileCommand,
}

#[derive(Subcommand)]
pub(crate) enum FileCommand {
    /// List a partition's files — the ones its commits added
    Ls(ls::LsArgs),
}

pub(crate) async fn run(lake: &pqbench::Lake, args: &FileArgs) -> Result<(), CliError> {
    match &args.command {
        FileCommand::Ls(ls) => ls::run(lake, ls).await,
    }
}
