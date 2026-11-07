//! `list catalogs`: the catalogs at the endpoint, one `Catalog` line each.

use clap::Args;
use pqbench::v1;

use crate::source::{drive, rows};
use crate::CliError;

/// Arguments for `list catalogs`.
#[derive(Args)]
pub(crate) struct ListArgs {}

/// List the catalogs at the endpoint: the walk's root, which needs no parent.
///
/// The metastore is a singleton, so there is no stream to shape this: it reads
/// nothing and writes the catalogs the endpoint serves. The daemon makes the
/// request, so the level is paced with every other catalog call.
pub(crate) async fn run(daemon: &crate::daemon::Daemon, _args: &ListArgs) -> Result<(), CliError> {
    let catalogs: Vec<v1::Catalog> = daemon.list_catalogs().await?;
    drive(rows(catalogs)).await
}
