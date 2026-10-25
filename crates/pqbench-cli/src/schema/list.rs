//! `list schemas`: the schemas in a catalog, one `Schema` line each.

use clap::Args;
use pqbench::schema::list;

use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::{self, Carried};
use crate::CliError;

/// Arguments for `list schemas`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// catalog name; without it, read `Catalog` refs on stdin
    catalog: Option<String>,
    /// catalogs in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &ListArgs) -> Result<(), CliError> {
    let names =
        source::parent_names("list schemas", args.catalog.as_deref(), Carried::Catalog).await?;
    let mut emit = Emitter::open();
    let dropped = source::stage(
        names,
        args.fan_out,
        &mut emit,
        |catalog: String| async move {
            list::list(workspace, &catalog)
                .await
                .map_err(|cause| context(&catalog, cause))
        },
    )
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}
