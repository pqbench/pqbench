//! `sample`: keep a rate of a collection.
//!
//! Sampling is a reduction over a stream, not a listing: it drops records, so
//! it is its own verb rather than a flag on `list`.

pub(crate) mod files;

use clap::{Args, Subcommand};

use crate::CliError;

/// Arguments for `sample`.
#[derive(Args)]
pub(crate) struct SampleArgs {
    #[command(subcommand)]
    collection: SampleCollection,
}

/// The collection a `sample` reduces.
#[derive(Subcommand)]
enum SampleCollection {
    /// Keep a rate of a table's files, by size
    Files(crate::sample::files::FilesArgs),
}

/// Sample a collection: the walk's sample verb.
pub(crate) async fn run(args: &SampleArgs) -> Result<(), CliError> {
    match &args.collection {
        SampleCollection::Files(args) => crate::sample::files::run(args).await,
    }
}

impl SampleArgs {
    /// The command's name, used in configuration errors.
    pub(crate) fn name(&self) -> &'static str {
        match self.collection {
            SampleCollection::Files(_) => "sample files",
        }
    }
}
