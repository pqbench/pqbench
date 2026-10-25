//! `list partitions`: a table's natural partitions, grouped by commit time.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use clap::Args;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use pqbench::partition;

use crate::emit::{Emitter, Format};
use crate::error::{error_chain, fail_when_dropped, tolerate};
use crate::source::{
    self, dotted_name, read_input, ref_credentials, ref_storage, table_ref, Input,
};
use crate::CliError;

/// Arguments for `list partitions`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// table URI or a document file; without it, read table refs on stdin
    input: Option<String>,
    /// window width, epoch-aligned: e.g. 1h, 1d, 1w [env: PQB_TABLE_EVERY]
    #[arg(
        long,
        value_parser = parse_every,
        value_name = "WINDOW",
        env = "PQB_TABLE_EVERY",
        default_value = "1d"
    )]
    every: i64,
    /// read only commits within WINDOW of the newest commit: e.g. 1d
    /// [env: PQB_TABLE_LAST]
    #[arg(long, value_parser = parse_last, value_name = "WINDOW", env = "PQB_TABLE_LAST")]
    last: Option<i64>,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// tables in flight at once [env: PQB_TABLE_FAN_OUT]
    #[arg(long, default_value_t = 64, env = "PQB_TABLE_FAN_OUT")]
    fan_out: usize,
}

/// List a table's natural partitions: commits grouped into commit-time windows.
pub(crate) async fn run(lake: &pqbench::Lake, args: &ListArgs) -> Result<(), CliError> {
    let input = read_input("list partitions").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let (partitions, dropped) = match &args.input {
        Some(uri) if uri != "-" => {
            if input.first.is_some() {
                return Err("list partitions takes a table URI or a table stream, not both".into());
            }
            let table = pqbench::v1::name::local_table(uri);
            (
                list_partitions(lake, &table, uri, args.every, args.last, &mut emit).await?,
                0,
            )
        }
        _ => list_stream(lake, input, args.every, args.last, args.fan_out, &mut emit).await?,
    };
    emit.finish(&format!("partitions: {partitions}\n")).await?;
    fail_when_dropped("table", dropped)
}

/// Emit every table's partitions from a table stream: tables in flight at
/// once, each streaming its partitions as its windows close.
async fn list_stream(
    lake: &pqbench::Lake,
    input: Input,
    window: i64,
    last: Option<i64>,
    fan_out: usize,
    emit: &mut Emitter,
) -> Result<(usize, usize), CliError> {
    if !input.piped {
        return Err("list partitions needs a table URI or a table stream".into());
    }
    let records = source::records("list partitions", input.first, input.lines);
    let dropped = Arc::new(AtomicUsize::new(0));
    let reads = records
        .map(|record| {
            let dropped = Arc::clone(&dropped);
            async move {
                let record = record?;
                let name = table_ref("list partitions", &record)?;
                let uri = ref_storage(&record)
                    .ok_or_else(|| {
                        CliError::from(
                            "a table-ref has no storage path; run `pqbench get table` first",
                        )
                    })?
                    .to_string();
                let lease = ref_credentials(&record).filter(|lease| !lease.is_empty());
                let env = lease
                    .as_ref()
                    .map(pqbench::v1::Credentials::to_env)
                    .unwrap_or_default();
                let label = dotted_name(&name);
                let found = partition::list::list(lake, &name, &uri, &env, window, last)
                    .await
                    .map_err(CliError::from);
                let Some(found) = tolerate(&label, found) else {
                    dropped.fetch_add(1, Ordering::Relaxed);
                    return Ok::<_, CliError>(
                        stream::empty::<Result<pqbench::partition::list::Partition, CliError>>()
                            .boxed(),
                    );
                };
                Ok(found
                    .map_ok(move |mut partition| {
                        partition.credentials = lease.clone();
                        partition
                    })
                    .map_err(CliError::from)
                    .filter_map(move |item| match item {
                        Ok(partition) => std::future::ready(Some(Ok(partition))),
                        Err(error) => {
                            eprintln!("error: {}", error_chain(error.as_ref()));
                            dropped.fetch_add(1, Ordering::Relaxed);
                            std::future::ready(None)
                        }
                    })
                    .boxed())
            }
        })
        .buffer_unordered(fan_out.max(1))
        .boxed();
    let mut found = reads.try_flatten_unordered(Some(fan_out.max(1)));
    let mut partitions = 0;
    while let Some(partition) = found.try_next().await? {
        emit.write_row(&partition).await?;
        partitions += 1;
    }
    Ok((partitions, dropped.load(Ordering::Relaxed)))
}

/// Emit one table's partitions as they arrive; returns how many were written.
async fn list_partitions(
    lake: &pqbench::Lake,
    table: &str,
    uri: &str,
    window: i64,
    last: Option<i64>,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let mut found = partition::list::list(lake, table, uri, &BTreeMap::new(), window, last)
        .await
        .map_err(CliError::from)?;
    let mut count = 0;
    while let Some(partition) = found.try_next().await.map_err(CliError::from)? {
        emit.write_row(&partition).await?;
        count += 1;
    }
    Ok(count)
}

/// Parse `--every`: a positive window width.
fn parse_every(value: &str) -> Result<i64, CliError> {
    parse_window("--every", value)
}

/// Parse `--last`: a positive read window.
fn parse_last(value: &str) -> Result<i64, CliError> {
    parse_window("--last", value)
}

/// Parse a window: a positive integer and a unit (`m`, `h`, `d`, `w`).
fn parse_window(flag: &str, value: &str) -> Result<i64, CliError> {
    let (number, unit) = value.split_at(
        value
            .len()
            .checked_sub(1)
            .ok_or_else(|| CliError::from(format!("{flag} is empty")))?,
    );
    let count: i64 = number
        .parse()
        .map_err(|_| CliError::from(format!("{flag} `{value}` needs a number and a unit")))?;
    if count <= 0 {
        return Err(format!("{flag} must be positive: {value}").into());
    }
    let millis = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return Err(format!("{flag} unit must be m, h, d, or w: {value}").into()),
    };
    Ok(count * millis)
}
