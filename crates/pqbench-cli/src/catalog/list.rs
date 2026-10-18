//! `list catalogs`: the catalogs at the endpoint, one `Catalog` line each.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::catalog::list;

use crate::emit::{Emitter, Format};
use crate::source::{self, read_input};
use crate::CliError;

/// Arguments for `list catalogs`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

/// List the catalogs at the endpoint: `get metastore | list catalogs`.
///
/// The metastore is a singleton, so a `Metastore` stream on standard input only
/// affords the walk its shape: the refs are validated, and the endpoint's
/// catalogs are the answer either way.
pub(crate) async fn run(workspace: &pqbench::Workspace, args: &ListArgs) -> Result<(), CliError> {
    let input = read_input("list catalogs").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if input.piped {
        let mut records = source::records("list catalogs", input.first, input.lines);
        while let Some(record) = records.next().await {
            source::metastore_ref("list catalogs", &record?)?;
        }
    }
    let catalogs = list::list(workspace).await?;
    for catalog in &catalogs {
        emit.write_row(catalog).await?;
    }
    emit.finish(&format!("catalogs: {}\n", catalogs.len()))
        .await
}
