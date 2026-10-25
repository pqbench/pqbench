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
    args: &GetArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Table>("get table").await?;
    drive(map(parents, args.fan_out, |table: v1::Table| async move {
        let lease = table.credentials.clone().filter(|lease| !lease.is_empty());
        let mut record = pqbench::table::get::get(workspace, lake, &table).await;
        if let Ok(record) = &mut record {
            record.credentials = lease;
        }
        record
    }))
    .await
}
