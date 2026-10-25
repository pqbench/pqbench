//! `get catalog`: one catalog's record.

use pqbench::v1;

use super::NameArgs;
use crate::parse;
use crate::source::{drive, map};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Catalog>("get catalog").await?;
    drive(map(
        parents,
        args.fan_out,
        |catalog: v1::Catalog| async move { pqbench::catalog::get::get(workspace, &catalog).await },
    ))
    .await
}
