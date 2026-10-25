//! `list catalogs`: the catalogs at the endpoint, one `Catalog` line each.

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::catalog::list;

use crate::emit::Emitter;
use crate::source::{self, read_input};
use crate::CliError;

/// Arguments for `list catalogs`.
#[derive(Args)]
pub(crate) struct ListArgs {}

/// List the catalogs at the endpoint: `get metastore | list catalogs`.
///
/// The metastore is a singleton, so a `Metastore` stream on standard input only
/// affords the walk its shape: the refs are validated, and the endpoint's
/// catalogs are the answer either way.
pub(crate) async fn run(workspace: &pqbench::Workspace, _args: &ListArgs) -> Result<(), CliError> {
    let input = read_input("list catalogs").await?;
    let mut emit = Emitter::open();
    if input.piped {
        let mut records = source::records("list catalogs", input.first, input.lines);
        while let Some(record) = records.next().await {
            source::Carried::Metastore.record_name(&record?)?;
        }
    }
    let catalogs = list::list(workspace).await?;
    for catalog in &catalogs {
        emit.write(catalog).await?;
    }
    emit.finish().await
}
