//! `list partitions`: a table's natural partitions, grouped by commit time.

use clap::Args;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use pqbench::partition;

use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::{self, dotted_name, read_input, ref_credentials, ref_storage};
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
    /// tables in flight at once [env: PQB_TABLE_FAN_OUT]
    #[arg(long, default_value_t = 64, env = "PQB_TABLE_FAN_OUT")]
    fan_out: usize,
}

/// List a table's natural partitions: commits grouped into commit-time windows.
pub(crate) async fn run(lake: &pqbench::Lake, args: &ListArgs) -> Result<(), CliError> {
    let input = read_input("list partitions").await?;
    let parents = match &args.input {
        Some(uri) if uri != "-" => {
            if input.first.is_some() {
                return Err("list partitions takes a table URI or a table stream, not both".into());
            }
            let name = pqbench::v1::name::local_table(uri);
            stream::iter([Ok(serde_json::json!({ "name": name, "uri": uri }))]).boxed()
        }
        _ => {
            if !input.piped {
                return Err("list partitions needs a table URI or a table stream".into());
            }
            source::records("list partitions", input.first, input.lines)
                .map(|record| {
                    record.and_then(|record| {
                        source::Carried::Table.record_name(&record).map(|_| record)
                    })
                })
                .boxed()
        }
    };
    let mut emit = Emitter::open();
    let dropped = source::stage(
        parents,
        args.fan_out,
        &mut emit,
        |record: serde_json::Value| async move {
            let name = source::Carried::Table.record_name(&record)?;
            let uri = ref_storage(&record)
                .ok_or_else(|| {
                    CliError::from("a table-ref has no storage path; run `pqbench get table` first")
                })?
                .to_string();
            let lease = ref_credentials(&record).filter(|lease| !lease.is_empty());
            let env = lease
                .as_ref()
                .map(pqbench::v1::Credentials::to_env)
                .unwrap_or_default();
            let mut found = partition::list::list(lake, &name, &uri, &env, args.every, args.last)
                .await
                .map_err(|cause| context(dotted_name(&name), cause))?;
            let mut partitions = Vec::new();
            while let Some(mut partition) = found
                .try_next()
                .await
                .map_err(|cause| context(dotted_name(&name), cause))?
            {
                partition.credentials = lease.clone();
                partitions.push(partition);
            }
            Ok(partitions)
        },
    )
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
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
