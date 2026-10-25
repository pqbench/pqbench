//! `list tables`: the tables in a schema, one `Table` line each.

use clap::Args;
use futures_util::TryStreamExt;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;
use futures_util::stream;

/// Arguments for `list tables`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// schemas in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &ListArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Schema>("list tables").await?;
    let workspace = workspace.clone();
    drive(flat_map(
        parents,
        args.fan_out,
        move |schema: v1::Schema| {
            let workspace = workspace.clone();
            async move {
                let rows = pqbench::table::list::list(&workspace, &schema)
                    .try_collect::<Vec<_>>()
                    .await?;
                Ok(stream::iter(
                    rows.into_iter().map(Ok::<_, std::convert::Infallible>),
                ))
            }
        },
    ))
    .await
}
