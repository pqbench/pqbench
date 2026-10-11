//! `ratelimit`: pace an NDJSON ref stream.
//!
//! A filter: records pass through unchanged, delayed so each record `kind`
//! observes at most `--rate` records per second. Nothing is dropped; the
//! consumer sees the pace if it issues one request per record as it arrives.

use std::time::Instant;

use clap::Args;
use pqbench::ratelimit::RateLimit;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::config::Sections;
use crate::CliError;

/// Arguments for `ratelimit`: the target rate.
#[derive(Args)]
pub(crate) struct RateLimitArgs {
    /// records per second per kind; 0 turns pacing off [env: PQB_RATE]
    #[arg(long, value_name = "RATE", env = "PQB_RATE")]
    rate: Option<f64>,
}

pub(crate) async fn run(sections: &Sections, args: &RateLimitArgs) -> Result<(), CliError> {
    let rate = rate(sections, args)?;
    if !rate.is_finite() || rate < 0.0 {
        return Err(format!("ratelimit expects a non-negative rate, got {rate}").into());
    }
    let mut limit = RateLimit::new(rate);
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let record: Value = serde_json::from_str(&line)
            .map_err(|error| format!("ratelimit reads NDJSON records: {error}"))?;
        let kind = record["kind"].as_str().unwrap_or_default();
        let delay = limit.delay(kind, Instant::now());
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

/// The rate: the flag, then `[ratelimit] rate`, then the default.
fn rate(sections: &Sections, args: &RateLimitArgs) -> Result<f64, CliError> {
    Ok(args
        .rate
        .or(sections.parse("ratelimit", "rate")?)
        .unwrap_or(pqbench::ratelimit::DEFAULT_REQUESTS_PER_SECOND))
}

async fn write_record(stdout: &mut tokio::io::Stdout, line: &str) -> std::io::Result<()> {
    stdout.write_all(line.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
}
