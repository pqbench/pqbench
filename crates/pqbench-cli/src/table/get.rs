//! `get table`: one table's record, read from its catalog and storage.

use clap::Args;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, map};
use crate::CliError;

/// The default tables in flight for `get table`: one per core.
fn default_fan_out() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Arguments for `get table`: the `catalog.schema.table` name and the output
/// flags.
#[derive(Args)]
pub(crate) struct GetArgs {
    /// tables in flight at once (default: cores) [env: PQB_TABLE_FAN_OUT]
    #[arg(long, default_value_t = default_fan_out(), env = "PQB_TABLE_FAN_OUT")]
    fan_out: usize,
}

pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &GetArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Table>("get table").await?;
    let workspace = workspace.clone();
    let lake = lake.clone();
    let daemon = daemon.clone();
    drive(map(parents, args.fan_out, move |table: v1::Table| {
        let workspace = workspace.clone();
        let lake = lake.clone();
        let daemon = daemon.clone();
        async move {
            // The catalog read runs through the daemon; the table's own
            // metadata is read from storage under the lease the daemon vends.
            let env = daemon.resolve_env(&table.name).await?;
            let credentials = v1::Credentials::from_env(&env);
            let lake = lake.with_lease(Some(&credentials));
            Ok::<_, CliError>(pqbench::table::get::get(&workspace, &lake, &table).await?)
        }
    }))
    .await
}
