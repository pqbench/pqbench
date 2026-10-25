//! `list files`: the files each partition's commits added.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use clap::Args;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use pqbench::v1;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::emit::{Emitter, Format};
use crate::error::{fail_when_dropped, ReadOutcome, RecordError};
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
    /// drop files whose logged size is below SIZE: bytes, or a number with a
    /// unit (kB/MB/GB/TB, KiB/MiB/GiB/TiB); e.g. 100MB
    #[arg(long, value_parser = parse_file_size, value_name = "SIZE")]
    min_file_size: Option<u64>,
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
    // commits are read; `started` counts the streams that opened so the summary
    // separates partitions from files.
    let started = Arc::new(AtomicUsize::new(0));
    let mut dropped_partitions = 0usize;
    let mut dropped_files = 0usize;
    let streams = partition_stream(lines)
        .map(|partition| {
            let started = Arc::clone(&started);
            async move {
                let partition = partition?;
                let v1::Partition {
                    name,
                    commit_versions,
                    uri,
                    credentials,
                    ..
                } = partition;
                let prepared = prepare_file_read(&name, uri, credentials)?;
                let found = pqbench::file::list::list(
                    lake,
                    &prepared.table,
                    &prepared.uri,
                    &prepared.env,
                    &commit_versions,
                )
                .await;
                Ok::<_, CliError>(match found {
                    Ok(found) => {
                        started.fetch_add(1, Ordering::Relaxed);
                        found
                            .map(move |item| {
                                Ok::<_, CliError>(match item {
                                    Ok(mut file) => {
                                        file.credentials = prepared.lease.clone();
                                        ReadOutcome::Record(file)
                                    }
                                    Err(error) => {
                                        ReadOutcome::Dropped(RecordError::new("file", &name, error))
                                    }
                                })
                            })
                            .boxed()
                    }
                    Err(error) => stream::iter([Ok::<_, CliError>(ReadOutcome::Dropped(
                        RecordError::new("partition", &name, error),
                    ))])
                    .boxed(),
                })
            }
        })
        .buffer_unordered(args.fan_out.max(1))
        .boxed();
    let mut found = streams.try_flatten_unordered(Some(args.fan_out.max(1)));
    while let Some(outcome) = found.try_next().await? {
        match outcome {
            ReadOutcome::Record(file) => {
                if !meets_min_file_size(&file, args.min_file_size) {
                    continue;
                }
                emit.write_row(&file).await?;
                files += 1;
            }
            ReadOutcome::Dropped(error) => {
                error.report();
                match error.what() {
                    "partition" => dropped_partitions += 1,
                    _ => dropped_files += 1,
                }
            }
        }
    }
    let partitions = started.load(Ordering::Relaxed);
    emit.finish(&format!("partitions: {partitions}\nfiles: {files}\n"))
        .await?;
    fail_when_dropped(&[("partition", dropped_partitions), ("file", dropped_files)])
}

/// The parts a partition's file read needs, after the record's own contract
/// checks: a bad name or a missing storage root fails the level, not the read.
struct FileRead {
    table: String,
    uri: String,
    lease: Option<v1::Credentials>,
    env: BTreeMap<String, String>,
}

fn prepare_file_read(
    name: &str,
    uri: Option<String>,
    credentials: Option<v1::Credentials>,
) -> Result<FileRead, CliError> {
    let (catalog, schema, table, _) =
        v1::name::parse_partition(name).ok_or_else(|| not_a_partition(name))?;
    let uri = uri.ok_or("a partition record needs a table storage root")?;
    let env = credentials
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    Ok(FileRead {
        table: v1::name::table(&catalog, &schema, &table),
        uri,
        lease: credentials,
        env,
    })
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

/// Whether a file's logged size clears `--min-file-size`. A file whose log
/// records no size cannot be shown to be big, so the floor drops it.
fn meets_min_file_size(file: &v1::TableFile, min_file_size: Option<u64>) -> bool {
    min_file_size.is_none_or(|min| file.size_bytes.is_some_and(|size| size >= min))
}

/// Parse `--min-file-size`: a byte count, or a number with a unit.
///
/// `kB`/`MB`/`GB`/`TB` are powers of 1000 and `KiB`/`MiB`/`GiB`/`TiB` powers
/// of 1024 (case-insensitive); a bare number is bytes.
fn parse_file_size(value: &str) -> Result<u64, CliError> {
    let digits = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(digits);
    let count: u64 = number
        .parse()
        .map_err(|_| CliError::from(format!("--min-file-size needs a number: {value}")))?;
    if count == 0 {
        return Err(format!("--min-file-size must be positive: {value}").into());
    }
    let multiplier: u64 = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        "kib" => 1_024,
        "mib" => 1_048_576,
        "gib" => 1_073_741_824,
        "tib" => 1_099_511_627_776,
        _ => {
            return Err(format!(
                "--min-file-size unit must be B, kB, MB, GB, TB, KiB, MiB, GiB, or TiB: {value}"
            )
            .into())
        }
    };
    count
        .checked_mul(multiplier)
        .ok_or_else(|| CliError::from(format!("--min-file-size is too large: {value}")))
}
