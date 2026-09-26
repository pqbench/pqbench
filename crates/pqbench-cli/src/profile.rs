use std::path::PathBuf;

use clap::Args;
use pqbench::profile::{self, ColumnProfile, ProfileRequest};
use pqbench::third_party::parquet::api::read_sample;
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
use crate::CliError;

/// Arguments for `profile`: read and decode a bounded row sample, then stream
/// one `pqbench.profile-column` line per column.
#[derive(Args)]
pub(crate) struct ProfileArgs {
    /// parquet paths to sample
    inputs: Vec<String>,
    /// also write the zstd NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// stream NDJSON (same as --format json; kept for scripts)
    #[arg(long = "json")]
    json: bool,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// column name glob, repeatable; default: every column
    #[arg(long = "columns", value_name = "GLOB")]
    columns: Vec<String>,
    /// how many leading rows to read: `all` or `first:N`
    #[arg(long = "rows", default_value = "first:8192", value_name = "METHOD")]
    rows: String,
    /// number of top values per column
    #[arg(long = "top", default_value_t = 8)]
    top: u32,
}

pub(crate) fn run(args: &ProfileArgs) -> Result<(), CliError> {
    if args.inputs.is_empty() {
        return Err("profile needs parquet files".into());
    }
    let max_rows = parse_rows(&args.rows)?;
    let request = ProfileRequest {
        columns: args.columns.clone(),
        top: args.top,
    };
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(args.json))?;
    emit.write_event(&BeginRecord {
        kind: "pqbench.profile",
        version: 1,
        event: "begin",
    })?;
    let mut column_count = 0usize;
    let mut row_count = 0u64;
    for input in &args.inputs {
        let sample = read_sample(std::path::Path::new(input), max_rows)?;
        let report = profile::profile(&sample, &request)?;
        row_count += report.row_count;
        for column in &report.columns {
            emit.write_row(&ColumnRecord {
                kind: "pqbench.profile-column",
                id: input,
                column,
            })?;
            column_count += 1;
        }
    }
    emit.write_event(&EndRecord {
        kind: "pqbench.profile",
        event: "end",
        file_count: args.inputs.len(),
        row_count,
        column_count,
    })?;
    let mut summary = format!(
        "files: {}\nrows: {}\ncolumns: {}\n",
        args.inputs.len(),
        row_count,
        column_count
    );
    if let Some(path) = &args.output {
        summary.push_str(&format!("output: {}\n", path.display()));
    }
    emit.finish(&summary)
}

/// Parse `--rows`: `all` reads every row, `first:N` reads N (N >= 1).
fn parse_rows(method: &str) -> Result<Option<usize>, CliError> {
    if method == "all" {
        return Ok(None);
    }
    if let Some(rest) = method.strip_prefix("first:") {
        let rows: usize = rest
            .parse()
            .map_err(|_| format!("bad --rows `{method}`; expected `all` or `first:N`"))?;
        if rows == 0 {
            return Err(format!("bad --rows `{method}`; N must be at least 1").into());
        }
        return Ok(Some(rows));
    }
    Err(format!("bad --rows `{method}`; expected `all` or `first:N`").into())
}

#[derive(Serialize)]
struct BeginRecord {
    kind: &'static str,
    version: u32,
    event: &'static str,
}

#[derive(Serialize)]
struct ColumnRecord<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(flatten)]
    column: &'a ColumnProfile,
}

impl Row for ColumnRecord<'_> {
    const HEADER: &'static [&'static str] = &["id", "column", "kind", "values", "nulls", "ndv"];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let column = self.column;
        vec![
            self.id.to_string(),
            column.column.clone(),
            column.physical_kind.clone(),
            column.num_values.to_string(),
            column.null_count.to_string(),
            column.ndv.to_string(),
        ]
    }
}

#[derive(Serialize)]
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    file_count: usize,
    row_count: u64,
    column_count: usize,
}
