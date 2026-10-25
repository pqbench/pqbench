//! `get table`: one table's record, read from its catalog and storage.

use clap::Args;
use pqbench::table::get;
use serde_json::Value;

use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::{self, dotted_name, ref_credentials, ref_storage, Carried};
use crate::CliError;

/// The default tables in flight for `get table`: one per core.
fn default_fan_out() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Arguments for `get table`: the `catalog.schema.table` name and the output
/// flags.
#[derive(Args)]
pub(crate) struct GetArgs {
    /// table name (`catalog.schema.table`); without it, read table refs on stdin
    table: Option<String>,
    /// tables in flight at once (default: cores) [env: PQB_TABLE_FAN_OUT]
    #[arg(long, default_value_t = default_fan_out(), env = "PQB_TABLE_FAN_OUT")]
    fan_out: usize,
}

pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &GetArgs,
) -> Result<(), CliError> {
    let parents = source::parents("get table", args.table.as_deref(), Carried::Table).await?;
    let mut emit = Emitter::open();
    let dropped = source::stage(
        parents,
        args.fan_out,
        &mut emit,
        |record: Value| async move {
            let name = record["name"].as_str().unwrap_or_default().to_owned();
            let uri = ref_storage(&record);
            let lease = ref_credentials(&record).filter(|lease| !lease.is_empty());
            let env = lease
                .as_ref()
                .map(pqbench::v1::Credentials::to_env)
                .unwrap_or_default();
            let mut table = get::get(workspace, lake, &name, uri, &env)
                .await
                .map_err(|cause| context(dotted_name(&name), cause))?;
            table.credentials = lease;
            Ok(vec![table])
        },
    )
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}
