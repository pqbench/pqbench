//! `list files`: a table's active files.

use clap::Args;
use futures_util::TryStreamExt;
use pqbench::v1;

use crate::source::{drive, flat_map};
use crate::time::WindowArgs;
use crate::{parse, CliError};

/// Arguments for `list files`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// tables in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
    /// read only files the log dates in a window
    #[command(flatten)]
    window: WindowArgs,
}

/// List a table's active files: `get table | list files [--last 1d] | bytemass`.
///
/// Reads Table refs on standard input, resolves each one's snapshot, and writes
/// its active files as they arrive — the files active now, not the ones some
/// commit added. The reader resolves the table's lease itself, so the emitted
/// records carry no credentials and `list columns` reads each one with its own
/// lookup. Tables are walked `--fan-out` at a time and a file is written as it
/// is produced, so the run holds one log batch plus one file per table in
/// flight, however large the tables are.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    leases: &crate::daemon::LeaseCache,
    args: &ListArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Table>("list files").await?;
    let window = args.window.window();
    let lake = lake.clone();
    let leases = leases.clone();
    drive(flat_map(parents, args.fan_out, move |table: v1::Table| {
        let lake = lake.clone();
        let leases = leases.clone();
        async move {
            let reference = table.name.clone();
            let env = leases.resolve_env(&reference).await?;
            let credentials = v1::Credentials::from_env(&env);
            let lake = lake.with_lease(Some(&credentials));
            let files = pqbench::file::list::list(&lake, &table, window)
                .await
                .map_err(|error| crate::error::context(&reference, error))?;
            // Each table's own failures stay attributed to it, mid-walk too.
            Ok(files.map_err(move |error| crate::error::context(&reference, error)))
        }
    }))
    .await
}
