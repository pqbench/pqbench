//! `metastore ls`: the catalogs at the endpoint.

use pqbench::metastore::ls;

use super::OutputArgs;
use crate::emit::Emitter;
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &OutputArgs) -> Result<(), CliError> {
    let catalogs = ls::list(workspace).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    for catalog in &catalogs {
        emit.write_row(catalog).await?;
    }
    emit.finish(&format!("catalogs: {}\n", catalogs.len()))
        .await
}
