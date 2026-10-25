//! `list tables`: the tables in a schema, one `Table` line each.

use clap::Args;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;

/// Arguments for `list tables`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// schemas in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &ListArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Schema>("list tables").await?;
    drive(flat_map(
        parents,
        args.fan_out,
        |schema: v1::Schema| async move { Ok(pqbench::table::list::list(workspace, &schema)) },
    ))
    .await
}
