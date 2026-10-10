use pqbench::lz::{self, LzRequest};
use serde::Serialize;

use crate::bench::BenchArgs;
use crate::emit::{Align, Emitter, Row};
use crate::CliError;

pub(crate) async fn run(args: &BenchArgs) -> Result<(), CliError> {
    let request = LzRequest {
        file: args.file.clone(),
        codec_specs: args.codec_specs.clone(),
        samples: args.samples,
        warmup_iterations: args.warmup_iterations,
        mode: args.mode.into(),
    };
    let report = lz::lz(&request)?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(args.json))?;
    for row in &report.rows {
        emit.write_row(&RowRecord {
            kind: "pqbench.lz-row",
            row,
        })
        .await?;
    }
    let mut summary = format!(
        "file: {}\nrows: {}\n",
        args.file.display(),
        report.rows.len()
    );
    if let Some(path) = &args.output {
        summary.push_str(&format!("output: {}\n", path.display()));
    }
    emit.finish(&summary).await
}

#[derive(Serialize)]
struct RowRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    row: &'a pqbench::report::ReportRow,
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
        let row = self.row;
        let bytes = row.uncompressed_bytes as u64;
        vec![
            row.codec.to_string(),
            row.level.to_string(),
            format!("{:.1}", row.compress_estimate.megabytes_per_second(bytes)),
            format!("{:.1}", row.decompress_estimate.megabytes_per_second(bytes)),
            format!("{:.2}", row.ratio),
        ]
    }
}
