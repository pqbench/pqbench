use clap::Args;
use pqbench::compression::{self, CompressionRequest};
use serde::Serialize;

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

pub(crate) fn run(args: &CompressionArgs) -> Result<(), CliError> {
    let _ = args.bench.json;
    let request = CompressionRequest {
        file: args.bench.file.clone(),
        codec_specs: args.bench.codec_specs.clone(),
        samples: args.bench.samples,
        warmup_iterations: args.bench.warmup_iterations,
        mode: args.bench.mode.into(),
        per_column: args.per_column,
    };
    let report = compression::compression(&request)?;
    let mut emit = Emitter::open("compression", args.bench.output.as_deref())?;
    emit.write(&BeginRecord {
        kind: "pqbench.compression",
        version: 1,
        event: "begin",
        file: args.bench.file.to_string_lossy(),
    })?;
    for row in &report.rows {
        emit.write(&RowRecord {
            kind: "pqbench.compression-row",
            row,
        })?;
    }
    for column in &report.columns {
        emit.write(&ColumnRecord {
            kind: "pqbench.compression-column",
            row: column,
        })?;
    }
    emit.write(&EndRecord {
        kind: "pqbench.compression",
        event: "end",
        row_count: report.rows.len(),
        column_count: report.columns.len(),
    })?;
    let mut summary = format!(
        "file: {}\nrows: {}\ncolumns: {}\n",
        args.bench.file.display(),
        report.rows.len(),
        report.columns.len()
    );
    if let Some(path) = &args.bench.output {
        summary.push_str(&format!("output: {}\n", path.display()));
    }
    emit.finish(&summary)
}

#[derive(Serialize)]
struct BeginRecord<'a> {
    kind: &'static str,
    version: u32,
    event: &'static str,
    file: std::borrow::Cow<'a, str>,
}

#[derive(Serialize)]
struct RowRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    row: &'a pqbench::report::ReportRow,
}

#[derive(Serialize)]
struct ColumnRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    row: &'a pqbench::report::ColumnRow,
}

#[derive(Serialize)]
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    row_count: usize,
    column_count: usize,
}
