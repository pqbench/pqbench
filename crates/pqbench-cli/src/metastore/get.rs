//! `get metastore`: the endpoint's metastore record.

use pqbench::metastore::get;

use super::OutputArgs;
use crate::emit::Emitter;
use crate::CliError;

pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    _args: &OutputArgs,
) -> Result<(), CliError> {
    let metastore = get::get(workspace).await?;
    let mut emit = Emitter::open();
    emit.write(&metastore)?;
    emit.finish()
}
