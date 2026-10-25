//! `credentials get`: put vended read credentials on each table-ref:
//! `list tables | credentials get`.
//!
//! The verb is `name -> lease`; the stage is what writes the lease onto the ref
//! it read, so the pipe carries on with the same record, enriched.

use pqbench::v1;

use super::StageArgs;
use crate::parse;
use crate::source::{drive, map};
use crate::CliError;

pub(super) async fn run(workspace: &pqbench::Workspace, args: &StageArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Table>("credentials get").await?;
    drive(map(parents, args.fan_out, |table: v1::Table| async move {
        let mut table = table;
        let credentials = pqbench::credentials::get::get(workspace, &table.name).await?;
        if let Some(credentials) = credentials {
            table.credentials = Some(credentials);
        }
        Ok::<_, pqbench::credentials::get::Error>(table)
    }))
    .await
}
