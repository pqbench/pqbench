//! `price`: what a collection's bytes cost in their storage classes.

pub(crate) mod partitions;

use clap::{Args, Subcommand};

use crate::CliError;

/// Arguments for `price`.
#[derive(Args)]
pub(crate) struct PriceArgs {
    #[command(subcommand)]
    collection: PriceCollection,
}

/// The collection a `price` prices.
#[derive(Subcommand)]
enum PriceCollection {
    /// Price each partition at its files' storage classes (Partition records on stdin)
    Partitions(crate::price::partitions::PartitionsArgs),
}

/// Price a collection: the walk's price verb.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &PriceArgs,
) -> Result<(), CliError> {
    match &args.collection {
        PriceCollection::Partitions(args) => {
            crate::price::partitions::run(lake, daemon, args).await
        }
    }
}

impl PriceArgs {
    /// The command's name, used in configuration errors.
    pub(crate) fn name(&self) -> &'static str {
        match self.collection {
            PriceCollection::Partitions(_) => "price partitions",
        }
    }
}
