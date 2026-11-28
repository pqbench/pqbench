//! `list catalogs`: the catalogs at the endpoint, one `Catalog` line each.

use clap::Args;
use futures_util::{StreamExt, TryStreamExt};

use crate::source::drive;
use crate::CliError;

/// Arguments for `list catalogs`.
#[derive(Args)]
pub(crate) struct ListArgs {}

/// List the catalogs at the endpoint: the walk's root, which needs no parent.
///
/// The metastore is a singleton, so there is no stream to shape this: it reads
/// nothing and writes the catalogs the endpoint serves. The workspace's catalog
/// GET is the daemon's, so the level is paced with every other catalog request.
pub(crate) async fn run(workspace: &pqbench::Workspace, _args: &ListArgs) -> Result<(), CliError> {
    let catalogs = pqbench::catalog::list::list(workspace)
        .err_into::<CliError>()
        .boxed_local();
    drive(catalogs).await
}
