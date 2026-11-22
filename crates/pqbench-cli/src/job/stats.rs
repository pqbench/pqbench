//! `job stats`: fold the master's partition versions into the totals the web
//! app serves.

use std::fs::File;
use std::io::{BufWriter, Write};

use clap::Args;

use pqbench::partition::stats::Summary;
use pqbench::v1::PartitionVersion;

use crate::daemon::protocol::Request;
use crate::CliError;

/// Arguments for `job stats`.
#[derive(Args, Default)]
pub(crate) struct StatsArgs {}

/// Write the totals of every partition version to the file the master gives,
/// then commit it: the same records `summarize partition-versions` writes.
pub(crate) async fn run(daemon: &crate::daemon::Daemon, _args: &StatsArgs) -> Result<(), CliError> {
    let _lock = super::lock(daemon, "stats")?;
    let path = daemon
        .master(&Request::StatsPath)
        .await?
        .path
        .ok_or("the master gave no stats path")?;
    let (count, mut lines) = daemon
        .master_list(&Request::ListPartitionVersions {
            parent: String::new(),
        })
        .await?;
    let written = write(&path, &mut lines).await?;
    if written != count {
        let _ = std::fs::remove_file(&path);
        return Err(
            format!("the master stopped after {written} of {count} partition versions").into(),
        );
    }
    daemon.master(&Request::Stats).await?;
    Ok(())
}

/// Fold the version lines into the summary, write it to `path` and sync it:
/// a reader sees the whole file or the old one, since the master renames it.
async fn write(
    path: &str,
    lines: &mut tokio::io::Lines<tokio::io::BufReader<tokio::net::UnixStream>>,
) -> Result<u64, CliError> {
    let mut summary = Summary::default();
    let mut read = 0;
    while let Some(line) = lines.next_line().await? {
        let version: PartitionVersion = serde_json::from_str(&line)?;
        summary.add(&version)?;
        read += 1;
    }
    let file = File::create(path).map_err(|error| format!("cannot write {path}: {error}"))?;
    let mut out = BufWriter::new(file);
    for stats in summary.into_stats() {
        serde_json::to_writer(&mut out, &stats)?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    out.get_ref()
        .sync_all()
        .map_err(|error| format!("cannot sync {path}: {error}"))?;
    Ok(read)
}
