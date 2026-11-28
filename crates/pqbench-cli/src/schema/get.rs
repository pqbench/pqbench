//! `get schema`: one schema's record.

use pqbench::v1;

use super::NameArgs;
use crate::parse;
use crate::source::{drive, map};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Schema>("get schema").await?;
    let workspace = workspace.clone();
    drive(map(parents, args.fan_out, move |schema: v1::Schema| {
        let workspace = workspace.clone();
        async move { Ok::<_, CliError>(pqbench::schema::get::get(&workspace, &schema).await?) }
    }))
    .await
}
