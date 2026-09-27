use clap::Args;
use pqbench::compression::{self, CompressionRequest};
use pqbench::report::{ColumnRow, ReportRow};
use serde::Serialize;

use crate::bench::BenchArgs;
use crate::emit::{Align, Emitter, Row};
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
    let resolved = args.bench.format.resolve(args.bench.json);
    let mut emit = Emitter::open(args.bench.output.as_deref(), resolved)?;
    emit.write_event(&BeginRecord {
        kind: "pqbench.compression",
        version: 1,
        event: "begin",
        file: args.bench.file.to_string_lossy(),
    })
    .await?;
    for row in &report.rows {
        emit.write_row(&RowRecord {
            kind: "pqbench.compression-row",
            row,
        })
        .await?;
    }
    for column in &report.columns {
        emit.write_row(&ColumnRecord {
            kind: "pqbench.compression-column",
            row: column,
        })
        .await?;
    }
    emit.write_event(&EndRecord {
        kind: "pqbench.compression",
        event: "end",
        row_count: report.rows.len(),
        column_count: report.columns.len(),
    })
    .await?;
    let mut summary = format!(
        "file: {}\nrows: {}\ncolumns: {}\n",
        args.bench.file.display(),
        report.rows.len(),
        report.columns.len()
    );
    if let Some(path) = &args.bench.output {
        summary.push_str(&format!("output: {}\n", path.display()));
    }
    emit.finish(&summary).await
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
    row: &'a ReportRow,
}

impl Row for RowRecord<'_> {
    const HEADER: &'static [&'static str] = &[
        "codec",
        "level",
        "compress MB/s",
        "decompress MB/s",
        "ratio",
    ];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        report_cells(self.row)
    }
}

#[derive(Serialize)]
struct ColumnRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    row: &'a ColumnRow,
}

impl Row for ColumnRecord<'_> {
    const HEADER: &'static [&'static str] = &[
        "codec",
        "level",
        "column",
        "compress MB/s",
        "decompress MB/s",
        "ratio",
    ];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Right,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let row = self.row;
        let bytes = row.uncompressed_bytes as u64;
        vec![
            row.codec.to_string(),
            row.level.to_string(),
            row.column.clone(),
            format!("{:.1}", row.compress_estimate.megabytes_per_second(bytes)),
            format!("{:.1}", row.decompress_estimate.megabytes_per_second(bytes)),
            format!("{:.2}", row.ratio),
        ]
    }
}

fn report_cells(row: &ReportRow) -> Vec<String> {
    let bytes = row.uncompressed_bytes as u64;
    vec![
        row.codec.to_string(),
        row.level.to_string(),
        format!("{:.1}", row.compress_estimate.megabytes_per_second(bytes)),
        format!("{:.1}", row.decompress_estimate.megabytes_per_second(bytes)),
        format!("{:.2}", row.ratio),
    ]
}

#[derive(Serialize)]
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    row_count: usize,
    column_count: usize,
}
