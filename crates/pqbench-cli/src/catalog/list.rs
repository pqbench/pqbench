//! `list catalogs`: the catalogs at the endpoint, one `Catalog` line each.

use clap::Args;
use futures_util::stream::StreamExt;
use futures_util::TryStreamExt;
use pqbench::v1;

use crate::error::context;
use crate::source::{drive, Records};
use crate::CliError;

/// Arguments for `list catalogs`.
#[derive(Args)]
pub(crate) struct ListArgs {}

/// List the catalogs at the endpoint: the walk's root, which needs no parent.
///
/// The metastore is a singleton, so there is no stream to shape this: it reads
/// nothing and writes the catalogs the endpoint serves.
pub(crate) async fn run(workspace: &pqbench::Workspace, _args: &ListArgs) -> Result<(), CliError> {
    let rows: Records<'_, v1::Catalog> = pqbench::catalog::list::list(workspace)
        .map_err(|cause| context("catalogs", cause))
        .boxed_local();
    drive(rows).await
}
