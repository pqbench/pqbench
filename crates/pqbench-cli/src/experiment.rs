use std::path::PathBuf;

use clap::Args;
use pqbench::experiment::{self, Aim, Experiment, ExperimentRequest, Trial};
use pqbench::third_party::parquet::api::read_typed_sample;
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
use crate::CliError;

/// Arguments for `experiment`: read a decoded row sample, apply each rewrite,
/// then stream one trial and one column record per measurement.
#[derive(Args)]
pub(crate) struct ExperimentArgs {
    /// parquet sample path to read
    input: String,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// stream NDJSON (same as --format json; kept for scripts)
    #[arg(long = "json")]
    json: bool,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// how many leading rows to read: `all` or `first:N`
    #[arg(long = "rows", default_value = "first:8192", value_name = "METHOD")]
    rows: String,
    /// rewrite spec, repeatable; semicolons compose one trial
    #[arg(long = "rewrite", value_name = "SPEC")]
    rewrites: Vec<String>,
    /// alias for `--rewrite`, repeatable
    #[arg(long = "trial", value_name = "SPEC")]
    trials: Vec<String>,
    /// what to measure: storage, skipping, all
    #[arg(long = "aim", default_value = "storage", value_name = "AIM")]
    aim: String,
}

pub(crate) async fn run(args: &ExperimentArgs) -> Result<(), CliError> {
    let max_rows = parse_rows(&args.rows)?;
    let mut trials = args.rewrites.clone();
    trials.extend(args.trials.iter().cloned());
    let request = ExperimentRequest {
        trials,
        aim: Aim::parse(&args.aim)?,
    };
    let sample = read_typed_sample(std::path::Path::new(&args.input), max_rows)?;
    let report = experiment::experiment(&sample, &request)?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(args.json))?;
    for trial in &report.trials {
        emit.write_row(&TrialRecord {
            kind: "pqbench.experiment-trial",
            trial,
        })
        .await?;
        for column in &trial.columns {
            emit.write_row(&ColumnRecord {
                kind: "pqbench.experiment-column",
                trial: trial.name.as_str(),
                column,
            })
            .await?;
        }
    }
    emit.finish(&summary(&report, args.output.as_deref())).await
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

fn summary(experiment: &Experiment, output: Option<&std::path::Path>) -> String {
    let mut out = format!(
        "rows: {}\naim: {}\ntrials: {}\n",
        experiment.row_count,
        experiment.aim.as_str(),
        experiment.trials.len()
    );
    if let Some(path) = output {
        out.push_str(&format!("output: {}\n", path.display()));
    }
    out
}

#[derive(Serialize)]
struct TrialRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    trial: &'a Trial,
}

impl Row for TrialRecord<'_> {
    const HEADER: &'static [&'static str] = &["trial", "bytes", "bytes/row", "row_groups", "ratio"];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
        Align::Right,
    ];

    fn cells(&self) -> Vec<String> {
        let trial = self.trial;
        vec![
            trial.name.clone(),
            trial.bytes.to_string(),
            format!("{:.2}", trial.bytes_per_row),
            trial.row_group_count.to_string(),
            trial
                .ratio
                .map_or_else(|| "-".to_string(), |ratio| format!("{ratio:.2}")),
        ]
    }
}

#[derive(Serialize)]
struct ColumnRecord<'a> {
    kind: &'static str,
    trial: &'a str,
    #[serde(flatten)]
    column: &'a experiment::ColumnTrial,
}

impl Row for ColumnRecord<'_> {
    const HEADER: &'static [&'static str] = &[
        "trial",
        "column",
        "codec",
        "bytes",
        "bytes/row",
        "dictionary",
    ];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Left,
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Left,
    ];

    fn cells(&self) -> Vec<String> {
        let column = self.column;
        vec![
            self.trial.to_string(),
            column.column.clone(),
            column.codec.clone(),
            column.compressed_bytes.to_string(),
            format!("{:.2}", column.bytes_per_row),
            column.dictionary.to_string(),
        ]
    }
}
