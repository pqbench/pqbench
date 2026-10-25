//! `list partitions`: a table's natural partitions, grouped by commit time.

use clap::Args;
use pqbench::v1;

use crate::parse;
use crate::source;
use crate::CliError;

/// Arguments for `list partitions`.
#[derive(Args)]
pub(crate) struct ListArgs {
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
    let parents = parse::parents::<v1::Table>("list partitions").await?;
    let rows = source::flat_map(parents, args.fan_out, |table: v1::Table| async move {
        pqbench::partition::list::list(lake, &table, args.every, args.last)
            .await
            .map_err(CliError::from)
    });
    source::drive(rows).await
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
