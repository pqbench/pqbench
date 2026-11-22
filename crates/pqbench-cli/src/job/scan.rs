//! `job scan`: list each table's partitions into the master.

use clap::Args;
use futures_util::StreamExt;
use pqbench::v1;

use crate::daemon::protocol::Request;
use crate::CliError;

/// Arguments for `job scan`.
#[derive(Args)]
pub(crate) struct ScanArgs {
    /// tables listed at once
    #[arg(long, default_value_t = 4)]
    fan_out: usize,
    /// standard input is the whole catalog walk: the rows of every table it
    /// does not name are deleted
    #[arg(long)]
    complete: bool,
}

/// Scan: `… | get table | job scan`. For each Table record, run `pqbench
/// list partitions` into the path the master gives, then apply it with one
/// `scan` call; a table whose listing fails is reported and nothing of it
/// moves. With `--complete`, the tables the input did not name are deleted
/// after the last one.
pub(crate) async fn run(daemon: &crate::daemon::Daemon, args: &ScanArgs) -> Result<(), CliError> {
    let _lock = super::lock(daemon, "scan")?;
    let socket = daemon
        .socket()
        .expect("the lock found the socket")
        .to_path_buf();
    let tables = crate::parse::parents::<v1::Table>("job scan").await?;
    let results: Vec<(Option<String>, Result<(), CliError>)> = tables
        .map(|table| {
            let socket = socket.clone();
            async move {
                let table = match table {
                    Ok(table) => table,
                    Err(error) => return (None, Err(error)),
                };
                let name = table.name.clone();
                let result = scan_table(daemon, &socket, &table).await;
                (
                    Some(name),
                    result.map_err(|error| crate::error::context(&table.name, error)),
                )
            }
        })
        .buffer_unordered(args.fan_out.max(1))
        .collect()
        .await;
    let mut tables = Vec::new();
    let mut failed = 0;
    for (name, result) in results {
        tables.extend(name);
        if let Err(error) = result {
            crate::error::report(&error);
            failed += 1;
        }
    }
    if args.complete {
        daemon.master(&Request::RetainTables { tables }).await?;
    }
    match failed {
        0 => Ok(()),
        failed => Err(format!("job scan: {failed} table(s) failed").into()),
    }
}

async fn scan_table(
    daemon: &crate::daemon::Daemon,
    socket: &std::path::Path,
    table: &v1::Table,
) -> Result<(), CliError> {
    let response = daemon
        .master(&Request::ScanPath {
            table: table.name.clone(),
        })
        .await?;
    let path = std::path::PathBuf::from(response.path.ok_or("the master gave no scan path")?);
    let input = format!("{}\n", serde_json::to_string(table)?);
    let socket = socket.to_path_buf();
    tokio::task::spawn_blocking(move || {
        super::run_command(&["list", "partitions"], &input, &path, &socket)
    })
    .await
    .map_err(|error| error.to_string())??;
    daemon
        .master(&Request::Scan {
            table: table.name.clone(),
        })
        .await?;
    Ok(())
}
