//! `get catalog`: one catalog's record.

use pqbench::v1;

use super::NameArgs;
use crate::parse;
use crate::source::{drive, map};
use crate::CliError;

pub(crate) async fn run(daemon: &crate::daemon::Daemon, args: &NameArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Catalog>("get catalog").await?;
    let daemon = daemon.clone();
    drive(map(parents, args.fan_out, move |catalog: v1::Catalog| {
        let daemon = daemon.clone();
        async move { daemon.get_catalog(&catalog).await }
    }))
    .await
}
