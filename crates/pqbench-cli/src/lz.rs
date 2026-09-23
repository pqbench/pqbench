use pqbench::lz::{self, LzRequest};
use serde::Serialize;

use crate::bench::BenchArgs;
use crate::emit::Emit;
use crate::CliError;

pub(crate) fn run(args: &BenchArgs) -> Result<(), CliError> {
    let _ = args.json;
    let request = LzRequest {
        file: args.file.clone(),
        codec_specs: args.codec_specs.clone(),
        samples: args.samples,
        warmup_iterations: args.warmup_iterations,
        mode: args.mode.into(),
    };
    let report = lz::lz(&request)?;
    let mut emit = Emit::open("lz", args.output.as_deref())?;
    emit.write(&BeginRecord {
        kind: "pqbench.lz",
        version: 1,
        event: "begin",
        file: args.file.to_string_lossy(),
    })?;
    for row in &report.rows {
        emit.write(&RowRecord {
            kind: "pqbench.lz-row",
            row,
        })?;
    }
    emit.write(&EndRecord {
        kind: "pqbench.lz",
        event: "end",
        row_count: report.rows.len(),
    })?;
    let mut summary = format!(
        "file: {}\nrows: {}\n",
        args.file.display(),
        report.rows.len()
    );
    if let Some(path) = &args.output {
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
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    row_count: usize,
}
