//! `summarize partition-versions`: fold the driver's partition versions into
//! one PartitionStats per catalog, schema and table.

use futures_util::TryStreamExt;

use pqbench::partition::stats::Summary;
use pqbench::v1;

use crate::emit::Emitter;
use crate::CliError;

const COMMAND: &str = "summarize partition-versions";

/// Read PartitionVersion records on standard input (from `list
/// partition-versions`) and write the totals in name order once the input
/// ends. A record that is not a partition version fails the run.
pub(crate) async fn run() -> Result<(), CliError> {
    let mut versions = crate::parse::parents::<v1::PartitionVersion>(COMMAND).await?;
    let mut summary = Summary::default();
    while let Some(version) = versions.try_next().await? {
        summary.add(&version)?;
    }
    let mut out = Emitter::open();
    for stats in summary.into_stats() {
        if out.write(&stats).await?.is_break() {
            break;
        }
    }
    out.finish().await
}
