//! `get catalog`: one catalog's record.

use pqbench::v1;

use super::NameArgs;
use crate::parse;
use crate::source::{drive, map};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Catalog>("get catalog").await?;
    let workspace = workspace.clone();
    drive(map(parents, args.fan_out, move |catalog: v1::Catalog| {
        let workspace = workspace.clone();
        async move { Ok::<_, CliError>(pqbench::catalog::get::get(&workspace, &catalog).await?) }
    }))
    .await
}
