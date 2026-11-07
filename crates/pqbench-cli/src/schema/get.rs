//! `get schema`: one schema's record.

use pqbench::v1;

use super::NameArgs;
use crate::parse;
use crate::source::{drive, map};
use crate::CliError;

pub(crate) async fn run(daemon: &crate::daemon::Daemon, args: &NameArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Schema>("get schema").await?;
    let daemon = daemon.clone();
    drive(map(parents, args.fan_out, move |schema: v1::Schema| {
        let daemon = daemon.clone();
        async move { daemon.get_schema(&schema).await }
    }))
    .await
}
