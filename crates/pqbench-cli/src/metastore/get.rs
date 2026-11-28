//! `get metastore`: the endpoint's metastore record.

use super::OutputArgs;
use crate::emit::Emitter;
use crate::CliError;

pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    _args: &OutputArgs,
) -> Result<(), CliError> {
    let metastore = pqbench::metastore::get::get(workspace).await?;
    let mut emit = Emitter::open();
    let _ = emit.write(&metastore).await?;
    emit.finish().await
}
