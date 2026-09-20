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
    per_column: bool,
}

pub(crate) fn run(args: &CompressionArgs) -> Result<(), CliError> {
    let request = CompressionRequest {
        file: args.bench.file.clone(),
        codec_specs: args.bench.codec_specs.clone(),
        samples: args.bench.samples,
        warmup_iterations: args.bench.warmup_iterations,
        mode: args.bench.mode.into(),
        per_column: args.per_column,
    };
    let report = compression::compression(&request)?;
    let output = if args.bench.json {
        compression::render_json(&report)?
    } else {
        compression::render_text(&report, args.per_column)?
    };
    print!("{output}");
    Ok(())
}
