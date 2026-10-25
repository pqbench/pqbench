//! `compression`: sweep codecs over a page set and report the best.

use clap::Args;
use pqbench::compression::{self, CompressionRequest};

use crate::bench::BenchArgs;
use crate::emit::Emitter;
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

pub(crate) async fn run(args: &CompressionArgs) -> Result<(), CliError> {
    let request = CompressionRequest {
        file: args.bench.file.clone(),
        codec_specs: args.bench.codec_specs.clone(),
        samples: args.bench.samples,
        warmup_iterations: args.bench.warmup_iterations,
        mode: args.bench.mode.into(),
        per_column: args.per_column,
    };
    let report = compression::compression(&request)?;
    let mut emit = Emitter::open();
    let blob = pqbench::v1::name::blob_name(&args.bench.file.display().to_string());
    for row in &report.rows {
        emit.write(&crate::render::SweepRow {
            name: format!("{blob}/compression/{}-{}", row.codec, row.level),
            row: row.clone(),
        })
        .await?;
    }
    for row in &report.columns {
        emit.write(&crate::render::ColumnSweepRow {
            name: format!(
                "{blob}/compression/{}-{}/columns/{}",
                row.codec, row.level, row.column
            ),
            row: row.clone(),
        })
        .await?;
    }
    emit.finish().await
}
