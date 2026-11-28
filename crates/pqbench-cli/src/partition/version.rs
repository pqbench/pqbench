//! `list partition-versions`: the driver's partition versions, one record
//! each.

use std::path::{Path, PathBuf};

use clap::Args;
use pqbench_driver::client;
use pqbench_driver::protocol::Request;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::CliError;

/// Arguments for `list partition-versions`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// only the versions under this catalog, schema, table or partition
    /// (`catalogs/…`); every version without it
    parent: Option<String>,
}

/// Arguments for `get partition-version`.
#[derive(Args)]
pub(crate) struct GetArgs {
    /// a version (`catalogs/…/partitions/…/versions/{etag}`), or a partition
    /// for its newest live version
    name: String,
}

/// The driver's socket, as `PQB_DRIVER_SOCK` names it.
fn socket() -> Result<PathBuf, CliError> {
    pqbench_driver::socket::from_env().ok_or_else(|| -> CliError {
        "the driver is not configured; set PQB_DRIVER_SOCK or run `pqbench-driver start --store DIR`"
            .into()
    })
}

/// A failed exchange, with the way to start the driver when none is there.
fn exchange_error(path: &Path, error: client::Error) -> CliError {
    match error {
        client::Error::Connect(_) => format!(
            "no driver is listening at {}; run `pqbench-driver start --store DIR`",
            path.display()
        )
        .into(),
        client::Error::Protocol(_) => error.to_string().into(),
    }
}

/// Read one version's stored results: its price, each suggestion, each column
/// mass, then the listing, one record per line.
pub(crate) async fn get(args: &GetArgs) -> Result<(), CliError> {
    let path = socket()?;
    let response = client::request(&path, &Request::partition_results(args.name.as_str()))
        .await
        .map_err(|error| exchange_error(&path, error))?;
    let records = match response.error {
        Some(error) => return Err(error.into()),
        None => response.records,
    };
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

/// List the driver's partition versions: one PartitionVersion per line, in
/// name order, as the driver holds them now.
pub(crate) async fn run(args: &ListArgs) -> Result<(), CliError> {
    let path = socket()?;
    let request = Request::list_partition_versions(args.parent.clone().unwrap_or_default());
    let (response, mut lines) = client::list(&path, &request)
        .await
        .map_err(|error| exchange_error(&path, error))?;
    let count = match response.error {
        Some(error) => return Err(error.into()),
        None => response.count.unwrap_or(0),
    };
    let mut out = BufWriter::new(tokio::io::stdout());
    let mut written = 0;
    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|error| exchange_error(&path, client::Error::Connect(error)))?
    {
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
            format!("the driver stopped after {written} of {count} partition versions").into(),
        );
    }
    Ok(())
}
