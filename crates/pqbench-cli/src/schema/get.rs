//! `get schema`: one schema's record.

use pqbench::v1;

use super::NameArgs;
use crate::parse;
use crate::source::{drive, map};
use crate::CliError;

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &NameArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Schema>("get schema").await?;
    drive(map(
        parents,
        args.fan_out,
        |schema: v1::Schema| async move { pqbench::schema::get::get(workspace, &schema).await },
    ))
    .await
}
