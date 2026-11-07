//! `get metastore`: the endpoint's metastore record.

use super::OutputArgs;
use crate::emit::Emitter;
use crate::CliError;

pub(crate) async fn run(
    daemon: &crate::daemon::Daemon,
    _args: &OutputArgs,
) -> Result<(), CliError> {
    let metastore = daemon.get_metastore().await?;
    let mut emit = Emitter::open();
    emit.write(&metastore).await?;
    emit.finish().await
}
