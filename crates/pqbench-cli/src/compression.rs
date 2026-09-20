use clap::Args;
use pqbench::compression::{self, CompressionRequest};

use crate::bench::BenchArgs;
use crate::CliError;

/// Arguments for `compression`: the shared sweep arguments plus the
/// compression-only per-column breakdown.
#[derive(Args)]
pub(crate) struct CompressionArgs {
    #[command(flatten)]
    bench: BenchArgs,
    /// report per-column breakdown
    #[arg(long = "per-column")]
    is_per_column: bool,
}

pub(crate) fn run(args: &CompressionArgs) -> Result<(), CliError> {
    let request = CompressionRequest {
        bench: args.bench.request(),
        per_column: args.is_per_column,
    };
    let report = compression::compression(&request)?;
    let output = if args.bench.is_json {
        compression::render_json(&report)?
    } else {
        compression::render_text(&report, args.is_per_column)
    };
    print!("{output}");
    Ok(())
}
