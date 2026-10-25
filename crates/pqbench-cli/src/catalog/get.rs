//! `get catalog`: one catalog's record.

use pqbench::catalog::get;

use super::NameArgs;
use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::{self, Carried};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let names =
        source::parent_names("get catalog", args.catalog.as_deref(), Carried::Catalog).await?;
    let mut emit = Emitter::open();
    let dropped = source::stage(names, args.fan_out, &mut emit, |name: String| async move {
        let record = get::get(workspace, &name)
            .await
            .map_err(|cause| context(&name, cause))?;
        Ok(vec![record])
    })
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}
