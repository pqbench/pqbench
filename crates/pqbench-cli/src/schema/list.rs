//! `list schemas`: the schemas in a catalog, one `Schema` line each.

use clap::Args;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;

/// Arguments for `list schemas`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// catalogs in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &ListArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Catalog>("list schemas").await?;
    drive(flat_map(
        parents,
        args.fan_out,
        |catalog: v1::Catalog| async move { Ok(pqbench::schema::list::list(workspace, &catalog)) },
    ))
    .await
}
