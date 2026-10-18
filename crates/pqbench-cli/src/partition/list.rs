//! `list partitions`: a table's natural partitions, grouped by commit time.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream::{StreamExt, TryStreamExt};
use pqbench::partition;

use crate::emit::{Emitter, Format};
use crate::source::{self, read_input, ref_credentials, ref_storage, table_ref, Input};
use crate::CliError;

/// Arguments for `list partitions`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// table URI or a document file; without it, read table refs on stdin
    input: Option<String>,
    /// window width, epoch-aligned: e.g. 1h, 1d, 1w [env: PQB_TABLE_EVERY]
    #[arg(
        long,
        value_parser = parse_window,
        value_name = "WINDOW",
        env = "PQB_TABLE_EVERY",
        default_value = "1d"
    )]
    every: i64,
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
    let partitions = match &args.input {
        Some(uri) if uri != "-" => {
            if input.first.is_some() {
                return Err("list partitions takes a table URI or a table stream, not both".into());
            }
            let table = pqbench::v1::name::local_table(uri);
            list_partitions(lake, &table, uri, None, args.every, &mut emit).await?
        }
        _ => list_stream(lake, input, args.every, args.fan_out, &mut emit).await?,
    };
    emit.finish(&format!("partitions: {partitions}\n")).await
}

/// Emit every table's partitions from a table stream: tables in flight at
/// once, each streaming its partitions as its windows close.
async fn list_stream(
    lake: &pqbench::Lake,
    input: Input,
    window: i64,
    fan_out: usize,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    if !input.piped {
        return Err("list partitions needs a table URI or a table stream".into());
    }
    let records = source::records("list partitions", input.first, input.lines);
    let reads = records
        .map(|record| async {
            let record = record?;
            let name = table_ref("list partitions", &record)?;
            let uri = ref_storage(&record)
                .ok_or_else(|| {
                    CliError::from("a table-ref has no storage path; run `pqbench get table` first")
                })?
                .to_string();
            let credentials = ref_credentials(&record);
            let found = partition::list::list(lake, &name, &uri, credentials.as_ref(), window)
                .await
                .map_err(|error| CliError::from(error.to_string()))?;
            Ok::<_, CliError>(
                found
                    .map_err(|error| CliError::from(error.to_string()))
                    .boxed(),
            )
        })
        .buffer_unordered(fan_out.max(1))
        .boxed();
    let mut found = reads.try_flatten_unordered(Some(fan_out.max(1)));
    let mut partitions = 0;
    while let Some(partition) = found.try_next().await? {
        emit.write_row(&partition).await?;
        partitions += 1;
    }
    Ok(partitions)
}

/// Emit one table's partitions as they arrive; returns how many were written.
async fn list_partitions(
    lake: &pqbench::Lake,
    table: &str,
    uri: &str,
    credentials: Option<&pqbench::v1::Credentials>,
    window: i64,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let mut found = partition::list::list(lake, table, uri, credentials, window)
        .await
        .map_err(|error| CliError::from(error.to_string()))?;
    let mut count = 0;
    while let Some(partition) = found
        .try_next()
        .await
        .map_err(|error| CliError::from(error.to_string()))?
    {
        emit.write_row(&partition).await?;
        count += 1;
    }
    Ok(count)
}

/// Parse a window width: a positive integer and a unit (`m`, `h`, `d`, `w`).
fn parse_window(value: &str) -> Result<i64, CliError> {
    let (number, unit) = value.split_at(
        value
            .len()
            .checked_sub(1)
            .ok_or_else(|| CliError::from("--every is empty"))?,
    );
    let count: i64 = number
        .parse()
        .map_err(|_| CliError::from(format!("--every `{value}` needs a number and a unit")))?;
    if count <= 0 {
        return Err(format!("--every must be positive: {value}").into());
    }
    let millis = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return Err(format!("--every unit must be m, h, d, or w: {value}").into()),
    };
    Ok(count * millis)
}
