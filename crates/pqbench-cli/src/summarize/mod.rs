//! `summarize`: fold a stream into a command document.
//!
//! A summary is a reduction over a stream, not a listing: it consumes the
//! measured records and writes one row per unit. It is its own verb rather
//! than a flag on `list`, because the output has a shape of its own.

pub(crate) mod columns;

use clap::{Args, Subcommand};

use crate::CliError;

/// Arguments for `summarize`.
#[derive(Args)]
pub(crate) struct SummarizeArgs {
    #[command(subcommand)]
    collection: SummarizeCollection,
}

/// The collection a `summarize` reduces.
#[derive(Subcommand)]
enum SummarizeCollection {
    /// Estimate each table column's mass from a bytemass stream
    Columns(crate::summarize::columns::ColumnsArgs),
}

/// Summarize a collection: the walk's summary verb.
pub(crate) async fn run(args: &SummarizeArgs) -> Result<(), CliError> {
    match &args.collection {
        SummarizeCollection::Columns(args) => crate::summarize::columns::run(args).await,
    }
}

impl SummarizeArgs {
    /// The command's name, used in configuration errors.
    pub(crate) fn name(&self) -> &'static str {
        match self.collection {
            SummarizeCollection::Columns(_) => "summarize columns",
        }
    }
}
