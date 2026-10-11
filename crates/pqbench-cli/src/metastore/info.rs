//! `metastore info`: the endpoint's metastore record.

use pqbench::metastore::info;

use super::OutputArgs;
use crate::emit::Emitter;
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &OutputArgs) -> Result<(), CliError> {
    let metastore = info::read(workspace).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    emit.write_row(&metastore).await?;
    emit.finish("metastores: 1\n").await
}
