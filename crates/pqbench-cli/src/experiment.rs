use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};

use clap::Args;
use pqbench::dump::{self, DumpFile, DumpRequest, RowGroups};
use pqbench::experiment::{self, Aim, Experiment, ExperimentRequest};
use serde::Serialize;

use crate::emit::Emit;
use crate::CliError;

/// Arguments for `experiment`.
#[derive(Args)]
pub(crate) struct ExperimentArgs {
    /// parquet sample paths, or `-` for a dump on standard input
    inputs: Vec<String>,
    /// write the zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// stream NDJSON (same as a pipe; kept for scripts)
    #[arg(long = "json")]
    json: bool,
    /// rows to read: all, first:N (default first:8192)
    #[arg(long, value_name = "METHOD", default_value = "first:8192")]
    rows: String,
    /// row groups to read from each file: all, first:N
    #[arg(long, value_name = "METHOD", default_value = "all")]
    row_groups: String,
    /// rewrite spec (repeatable). Each value is one trial (`sort:a;codec:zstd@3`).
    #[arg(long, value_name = "SPEC")]
    rewrite: Vec<String>,
    /// alias for `--rewrite` (repeatable)
    #[arg(long, value_name = "SPEC")]
    trial: Vec<String>,
    /// what to measure: storage, skipping, all (default storage)
    #[arg(long, value_name = "AIM", default_value = "storage")]
    aim: String,
    /// also load page indexes from the rewritten file
    #[arg(long)]
    indexes: bool,
}

pub(crate) fn run(args: &ExperimentArgs) -> Result<(), CliError> {
    let _ = args.json;
    let max_rows = parse_rows(&args.rows)?;
    let row_groups = RowGroups::parse(&args.row_groups)?;
    let mut trials = args.rewrite.clone();
    trials.extend(args.trial.iter().cloned());
    let request = ExperimentRequest {
        trials,
        aim: Aim::parse(&args.aim)?,
        indexes: args.indexes,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let dump = runtime.block_on(load_sample(args, row_groups, max_rows))?;
    let experiment = experiment::experiment(&dump, &request)?;
    write_experiment(args, &experiment)
}

async fn load_sample(
    args: &ExperimentArgs,
    row_groups: RowGroups,
    max_rows: Option<usize>,
) -> Result<pqbench::dump::Dump, CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err("experiment needs a parquet sample or a dump on standard input".into());
        }
        return read_stdin(max_rows);
    }
    if args.inputs.len() == 1 && args.inputs[0] == "-" {
        return read_stdin(max_rows);
    }
    let files = args
        .inputs
        .iter()
        .map(|input| DumpFile {
            path: input.clone(),
            uri: input.clone(),
            table: None,
            env: Default::default(),
        })
        .collect();
    Ok(dump::sample(&DumpRequest { files, row_groups }, max_rows).await?)
}

fn read_stdin(max_rows: Option<usize>) -> Result<pqbench::dump::Dump, CliError> {
    let mut bytes = Vec::new();
    std::io::stdin().read_to_end(&mut bytes)?;
    if looks_like_json(&bytes) {
        return Err(
            "experiment reads a Parquet sample; pipe `pqbench dump` or pass a .parquet file".into(),
        );
    }
    Ok(dump::sample_bytes(&bytes, max_rows)?)
}

fn looks_like_json(bytes: &[u8]) -> bool {
    let start = bytes.iter().find(|byte| !byte.is_ascii_whitespace());
    matches!(start, Some(b'{') | Some(b'['))
}

fn parse_rows(value: &str) -> Result<Option<usize>, CliError> {
    match pqbench::pattern::Sample::parse(value)? {
        pqbench::pattern::Sample::ALL => Ok(None),
        pqbench::pattern::Sample::First(count) => Ok(Some(count as usize)),
        pqbench::pattern::Sample::Every(_) => {
            Err("rows does not support every:N; expected all or first:N".into())
        }
    }
}

fn write_experiment(args: &ExperimentArgs, experiment: &Experiment) -> Result<(), CliError> {
    let mut emit = Emit::open("experiment", args.output.as_deref())?;
    emit.write(&BeginRecord {
        kind: "pqbench.experiment",
        version: 1,
        event: "begin",
        num_rows: experiment.num_rows,
        aim: experiment.aim.as_str(),
        capabilities: &experiment.capabilities,
    })?;
    for trial in &experiment.trials {
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
        trial_count: experiment.trials.len(),
        num_rows: experiment.num_rows,
    })?;
    emit.finish(&summary(experiment, args.output.as_deref()))
}

fn summary(experiment: &Experiment, output: Option<&Path>) -> String {
    let mut out = format!(
        "rows: {}\naim: {}\ntrials: {}\n",
        experiment.num_rows,
        experiment.aim,
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
    num_rows: u64,
    aim: &'a str,
    capabilities: &'a [pqbench::experiment::Capability],
}

#[derive(Serialize)]
struct TrialRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    trial: &'a pqbench::experiment::Trial,
}

#[derive(Serialize)]
struct ColumnRecord<'a> {
    kind: &'static str,
    trial: &'a str,
    #[serde(flatten)]
    column: &'a pqbench::experiment::ColumnTrial,
}

#[derive(Serialize)]
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    trial_count: usize,
    num_rows: u64,
}
