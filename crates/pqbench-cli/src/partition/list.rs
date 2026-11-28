//! `list partitions`: a table's partitions, one record each.

use clap::Args;
use futures_util::stream;
use pqbench::v1;

use crate::source::{drive, flat_map};
use crate::{parse, CliError};

/// Arguments for `list partitions`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// tables in flight at once
    #[arg(long, default_value_t = 4)]
    fan_out: usize,
}

/// List a table's partitions: `get table | list partitions`.
///
/// Reads Table records on standard input (from `get table`, which names the
/// partition columns a partition's name follows), reads each one's snapshot
/// once, and writes one Partition per partition, sorted by name, when the
/// table is done.
/// A table holds one accumulator per partition while it is read, never its
/// files, so the run holds that plus one log batch per table in flight;
/// `--fan-out` bounds the tables in flight. A table that fails is reported
/// and writes nothing; the others still do.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &ListArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Table>("list partitions").await?;
    drive(flat_map(
        parents,
        args.fan_out,
        move |table: v1::Table| async move {
            let reference = table.name.clone();
            let env = daemon.resolve_env(&reference).await?;
            let credentials = v1::Credentials::from_env(&env);
            let lake = lake.with_lease(Some(&credentials));
            let partitions = pqbench::partition::list::list(&lake, &table)
                .await
                .map_err(|error| crate::error::context(&reference, error))?;
            Ok(stream::iter(partitions.into_iter().map(Ok::<_, CliError>)))
        },
    ))
    .await
}
