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
