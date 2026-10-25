//! `list tables`: the tables in a schema, one `Table` line each.

use clap::Args;
use pqbench::table::list;

use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::{self, Carried};
use crate::CliError;

/// Arguments for `list tables`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// schema name (`catalog.schema`); without it, read schema refs on stdin
    schema: Option<String>,
    /// schemas in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &ListArgs) -> Result<(), CliError> {
    let names =
        source::parent_names("list tables", args.schema.as_deref(), Carried::Schema).await?;
    let mut emit = Emitter::open();
    let dropped = source::stage(
        names,
        args.fan_out,
        &mut emit,
        |schema: String| async move {
            list::list(workspace, &schema)
                .await
                .map_err(|cause| context(&schema, cause))
        },
    )
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}
