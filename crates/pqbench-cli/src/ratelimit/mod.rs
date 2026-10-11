//! `ratelimit`: pace an NDJSON ref stream.
//!
//! A filter: records pass through unchanged, delayed so each resource type
//! observes at most `--rate` records per second. Nothing is dropped; the
//! consumer sees the pace if it issues one request per record as it arrives.

use std::time::Instant;

use clap::Args;
use pqbench::ratelimit::RateLimit;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::CliError;

/// Arguments for `ratelimit`: the target rate.
#[derive(Args)]
pub(crate) struct RateLimitArgs {
    /// records per second per resource type; 0 turns pacing off [env: PQB_RATE]
    #[arg(long, value_name = "RATE", env = "PQB_RATE", value_parser = parse_rate, allow_negative_numbers = true, default_value_t = pqbench::ratelimit::DEFAULT_REQUESTS_PER_SECOND)]
    rate: f64,
}

/// The resource type a record names — the pacing bucket: the collection
/// segment of its `name` (the segment before the id), or its `kind` for a
/// document that still carries one. A record that names neither shares the
/// empty bucket.
fn record_type(record: &Value) -> &str {
    if let Some(kind) = record.get("kind").and_then(Value::as_str) {
        return kind;
    }
    let Some(name) = record.get("name").and_then(Value::as_str) else {
        return "";
    };
    let segments: Vec<&str> = name.split('/').collect();
    segments
        .len()
        .checked_sub(2)
        .and_then(|index| segments.get(index))
        .copied()
        .unwrap_or("")
}

/// Parse `--rate`: a finite, non-negative requests-per-second.
fn parse_rate(value: &str) -> Result<f64, CliError> {
    let rate: f64 = value
        .parse()
        .map_err(|_| format!("--rate expects a number, got {value:?}"))?;
    if !rate.is_finite() || rate < 0.0 {
        return Err(format!("--rate expects a non-negative rate, got {rate}").into());
    }
    Ok(rate)
}

pub(crate) async fn run(args: &RateLimitArgs) -> Result<(), CliError> {
    let mut limit = RateLimit::new(args.rate);
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(&line)
            .map_err(|error| format!("ratelimit reads NDJSON records: {error}"))?;
        let resource = record_type(&record);
        let delay = limit.delay(resource, Instant::now());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if let Err(error) = write_record(&mut stdout, &line).await {
            if error.kind() == std::io::ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(error.into());
        }
    }
    Ok(())
}

async fn write_record(stdout: &mut tokio::io::Stdout, line: &str) -> std::io::Result<()> {
    stdout.write_all(line.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
}
