use std::path::PathBuf;

use clap::Args;
use pqbench::experiment::{self, Aim, Experiment, ExperimentRequest, Trial};
use pqbench::third_party::parquet::api::read_typed_sample;
use serde::Serialize;

use crate::emit::Emitter;
use crate::CliError;

/// Arguments for `experiment`: read a decoded row sample, apply each rewrite,
/// then stream one trial and one column record per measurement.
#[derive(Args)]
pub(crate) struct ExperimentArgs {
    /// parquet sample path to read
    input: String,
    /// write the zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// stream NDJSON (same as a pipe; kept for scripts)
    #[arg(long = "json")]
    json: bool,
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

pub(crate) fn run(args: &ExperimentArgs) -> Result<(), CliError> {
    let _ = args.json;
    let max_rows = parse_rows(&args.rows)?;
    let mut trials = args.rewrites.clone();
    trials.extend(args.trials.iter().cloned());
    let request = ExperimentRequest {
        trials,
        aim: Aim::parse(&args.aim)?,
    };
    let sample = read_typed_sample(std::path::Path::new(&args.input), max_rows)?;
    let report = experiment::experiment(&sample, &request)?;
    let mut emit = Emitter::open("experiment", args.output.as_deref())?;
    emit.write(&BeginRecord {
        kind: "pqbench.experiment",
        version: 1,
        event: "begin",
        row_count: report.row_count,
        aim: report.aim.as_str(),
        capabilities: &report.capabilities,
    })?;
    for trial in &report.trials {
        emit.write(&TrialRecord {
            kind: "pqbench.experiment-trial",
            trial,
        })?;
        for column in &trial.columns {
            emit.write(&ColumnRecord {
                kind: "pqbench.experiment-column",
                trial: trial.name.as_str(),
                column,
            })?;
        }
    }
    emit.write(&EndRecord {
        kind: "pqbench.experiment",
        event: "end",
        trial_count: report.trials.len(),
        row_count: report.row_count,
    })?;
    emit.finish(&summary(&report, args.output.as_deref()))
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
struct BeginRecord<'a> {
    kind: &'static str,
    version: u32,
    event: &'static str,
    row_count: u64,
    aim: &'a str,
    capabilities: &'a [pqbench::experiment::Capability],
}

#[derive(Serialize)]
struct TrialRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    trial: &'a Trial,
}

#[derive(Serialize)]
struct ColumnRecord<'a> {
    kind: &'static str,
    trial: &'a str,
    #[serde(flatten)]
    column: &'a experiment::ColumnTrial,
}

#[derive(Serialize)]
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    trial_count: usize,
    row_count: u64,
}
