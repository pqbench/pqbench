//! `list files`: the files each partition's commits added.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use clap::Args;
use futures_util::stream::{self, BoxStream, StreamExt, TryStreamExt};
use pqbench::v1;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::emit::{Emitter, Format};
use crate::stdin::is_stream;
use crate::CliError;

/// Arguments for `list files`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// partitions in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

/// List the files each partition's commits added: `list partitions | list files`.
///
/// Reads a partition stream, re-reads each table's log, and emits the files the
/// window's commits named — the **added in window** set, so a file a later
/// commit removes is still named. Each file is a [`v1::TableFile`] naming its
/// blob and carrying the partition's lease, so the storage stage reads it
/// without a catalog call.
pub(crate) async fn run(lake: &pqbench::Lake, args: &ListArgs) -> Result<(), CliError> {
    if !is_stream() {
        return Err("list files reads partition records on standard input".into());
    }
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let lines = BufReader::new(tokio::io::stdin()).lines();
    let mut files = 0usize;
    // Partition streams in flight at once, each streaming its files as its
    // commits are read; `started` counts them so the summary separates
    // partitions from files.
    let started = Arc::new(AtomicUsize::new(0));
    let streams = partition_stream(lines)
        .map(|partition| async move { file_rows(lake, partition?).await })
        .buffer_unordered(args.fan_out.max(1))
        .inspect_ok({
            let started = Arc::clone(&started);
            move |_| {
                started.fetch_add(1, Ordering::Relaxed);
            }
        })
        .boxed();
    let mut found = streams.try_flatten_unordered(Some(args.fan_out.max(1)));
    while let Some(file) = found.try_next().await? {
        emit.write_row(&file).await?;
        files += 1;
    }
    let partitions = started.load(Ordering::Relaxed);
    emit.finish(&format!("partitions: {partitions}\nfiles: {files}\n"))
        .await
}

/// One partition's rows: the files its commits added, each becoming a table
/// file as it arrives — so the first file is written while the window's later
/// commits are still being read.
async fn file_rows(
    lake: &pqbench::Lake,
    partition: v1::Partition,
) -> Result<BoxStream<'static, Result<v1::TableFile, CliError>>, CliError> {
    let (catalog, schema, table, _) = v1::name::parse_partition(&partition.name)
        .ok_or_else(|| not_a_partition(&partition.name))?;
    let uri = partition
        .uri
        .clone()
        .ok_or("a partition record needs a table storage root")?;
    let lease = partition.credentials;
    let env = lease
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    let table = v1::name::table(&catalog, &schema, &table);
    let found = pqbench::file::list::list(lake, &table, &uri, &env, &partition.commit_versions)
        .await
        .map_err(CliError::from)?;
    Ok(found
        .map_ok(move |mut file| {
            file.credentials = lease.clone();
            file
        })
        .map_err(CliError::from)
        .boxed())
}

/// The error for a partition record whose name is not a partition resource.
fn not_a_partition(name: &str) -> CliError {
    format!("a partition name must be `…/tables/{{table}}/partitions/{{partition}}`: {name}").into()
}

/// Read partition records off a line stream, one at a time.
fn partition_stream(
    lines: tokio::io::Lines<BufReader<tokio::io::Stdin>>,
) -> impl stream::Stream<Item = Result<v1::Partition, CliError>> + Unpin {
    Box::pin(stream::try_unfold(lines, |mut lines| async move {
        loop {
            let Some(line) = lines.next_line().await? else {
                return Ok(None);
            };
            if line.trim().is_empty() {
                continue;
            }
            let partition: v1::Partition = serde_json::from_str(&line).map_err(|error| {
                CliError::from(format!("list files reads partition NDJSON: {error}"))
            })?;
            if partition.uri.is_none() {
                return Err("a partition record needs a table storage root".into());
            }
            return Ok(Some((partition, lines)));
        }
    }))
}
