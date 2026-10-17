//! `list partitions`: a table's natural partitions, grouped by commit time.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use futures_util::stream::{StreamExt, TryStreamExt};
use pqbench::partition;

use crate::emit::{Emitter, Format};
use crate::source::{self, read_input, ref_env, ref_storage, table_ref};
use crate::CliError;

/// Arguments for `list partitions`.
#[derive(Args)]
pub(crate) struct LsArgs {
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
pub(crate) async fn run(lake: &pqbench::Lake, args: &LsArgs) -> Result<(), CliError> {
    let input = read_input("list partitions").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let mut partitions = 0usize;
    match &args.input {
        Some(uri) if uri != "-" => {
            if input.first.is_some() {
                return Err("list partitions takes a table URI or a table stream, not both".into());
            }
            let env = BTreeMap::new();
            let table = pqbench::v1::name::local_table(uri);
            partitions += list_partitions(lake, &table, uri, &env, args.every, &mut emit).await?;
        }
        _ => {
            if !input.piped {
                return Err("list partitions needs a table URI or a table stream".into());
            }
            let records = source::records("list partitions", input.first, input.lines);
            let reads = records
                .map(|record| async {
                    let record = record?;
                    let (catalog, schema, table) = table_ref("list partitions", &record)?;
                    let name = pqbench::v1::name::table(&catalog, &schema, &table);
                    let uri = ref_storage(&record)
                        .ok_or_else(|| {
                            CliError::from(
                                "a table-ref has no storage path; run `pqbench get table` first",
                            )
                        })?
                        .to_string();
                    let env = ref_env(&record);
                    let found = partition::ls::list(lake, &name, &uri, &env, args.every)
                        .await
                        .map_err(|error| CliError::from(error.to_string()))?;
                    Ok::<_, CliError>(
                        found
                            .map_err(|error| CliError::from(error.to_string()))
                            .boxed(),
                    )
                })
                .buffer_unordered(args.fan_out.max(1))
                .boxed();
            // Tables in flight at once, each streaming its partitions as its
            // windows close.
            let mut found = reads.try_flatten_unordered(Some(args.fan_out.max(1)));
            while let Some(partition) = found.try_next().await? {
                emit.write_row(&partition).await?;
                partitions += 1;
            }
        }
    }
    emit.finish(&format!("partitions: {partitions}\n")).await
}

/// Emit one table's partitions as they arrive; returns how many were written.
async fn list_partitions(
    lake: &pqbench::Lake,
    table: &str,
    uri: &str,
    env: &BTreeMap<String, String>,
    window: i64,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let mut found = partition::ls::list(lake, table, uri, env, window)
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
