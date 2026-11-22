//! `list partition-versions`: the master's partition versions, one record
//! each.

use clap::Args;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::daemon::protocol::Request;
use crate::CliError;

/// Arguments for `list partition-versions`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// only the versions under this catalog, schema, table or partition
    /// (`catalogs/…`; `-` for any id); every version without it
    parent: Option<String>,
}

/// Arguments for `get partition-version`.
#[derive(Args)]
pub(crate) struct GetArgs {
    /// a version (`catalogs/…/partitions/…/versions/{etag}`), or a partition
    /// for its newest live version
    name: String,
}

/// Read one version's stored results: its price, each suggestion, each column
/// mass, then the listing, one record per line.
pub(crate) async fn get(daemon: &crate::daemon::Daemon, args: &GetArgs) -> Result<(), CliError> {
    let request = Request::PartitionResults {
        name: args.name.clone(),
    };
    let records = daemon.master_results(&request).await?;
    let mut out = BufWriter::new(tokio::io::stdout());
    for record in &records {
        let mut line = serde_json::to_string(record)?;
        line.push('\n');
        match out.write_all(line.as_bytes()).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
    match out.flush().await {
        Err(error) if error.kind() != std::io::ErrorKind::BrokenPipe => Err(error.into()),
        _ => Ok(()),
    }
}

/// List the master's partition versions: one PartitionVersion per line, in
/// name order, as the daemon's master holds them now.
pub(crate) async fn run(daemon: &crate::daemon::Daemon, args: &ListArgs) -> Result<(), CliError> {
    let request = Request::ListPartitionVersions {
        parent: args.parent.clone().unwrap_or_default(),
    };
    let (count, mut lines) = daemon.master_list(&request).await?;
    let mut out = BufWriter::new(tokio::io::stdout());
    let mut written = 0;
    while let Some(line) = lines.next_line().await? {
        let result = match out.write_all(line.as_bytes()).await {
            Ok(()) => out.write_all(b"\n").await,
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => written += 1,
            Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
    match out.flush().await {
        Err(error) if error.kind() != std::io::ErrorKind::BrokenPipe => return Err(error.into()),
        _ => {}
    }
    if written != count {
        return Err(
            format!("the daemon stopped after {written} of {count} partition versions").into(),
        );
    }
    Ok(())
}
