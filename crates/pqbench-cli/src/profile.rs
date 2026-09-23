use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};

use clap::Args;
use pqbench::dump::{self, DumpFile, DumpRequest, RowGroups};
use pqbench::profile::{self, Profile, ProfileRequest};
use serde::Serialize;

use crate::emit::Emit;
use crate::CliError;

/// Arguments for `profile`.
#[derive(Args)]
pub(crate) struct ProfileArgs {
    /// parquet sample paths, or `-` for a dump on standard input
    inputs: Vec<String>,
    /// write the zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// stream NDJSON (same as a pipe; kept for scripts)
    #[arg(long = "json")]
    json: bool,
    /// keep columns whose names match this glob (repeatable)
    #[arg(long, value_name = "GLOB")]
    columns: Vec<String>,
    /// rows to read: all, first:N (default first:8192)
    #[arg(long, value_name = "METHOD", default_value = "first:8192")]
    rows: String,
    /// row groups to read from each file: all, first:N
    #[arg(long, value_name = "METHOD", default_value = "all")]
    row_groups: String,
    /// heavy-hitter values to keep per column
    #[arg(long, default_value = "8", value_name = "N")]
    top: u32,
    /// also emit pairwise dependency facts (O(columns² · rows))
    #[arg(long)]
    dependencies: bool,
}

pub(crate) fn run(args: &ProfileArgs) -> Result<(), CliError> {
    let _ = args.json;
    let max_rows = parse_rows(&args.rows)?;
    let row_groups = RowGroups::parse(&args.row_groups)?;
    let request = ProfileRequest {
        columns: args.columns.clone(),
        top: args.top.max(1),
        dependencies: args.dependencies,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let dump = runtime.block_on(load_sample(args, row_groups, max_rows))?;
    let profile = profile::profile(&dump, &request)?;
    write_profile(args, &profile)
}

async fn load_sample(
    args: &ProfileArgs,
    row_groups: RowGroups,
    max_rows: Option<usize>,
) -> Result<pqbench::dump::Dump, CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err("profile needs a parquet sample or a dump on standard input".into());
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
            "profile reads a Parquet sample; pipe `pqbench dump` or pass a .parquet file".into(),
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

fn write_profile(args: &ProfileArgs, profile: &Profile) -> Result<(), CliError> {
    let mut emit = Emit::open("profile", args.output.as_deref())?;
    emit.write(&BeginRecord {
        kind: "pqbench.profile",
        version: 1,
        event: "begin",
        num_rows: profile.num_rows,
        capabilities: &profile.capabilities,
    })?;
    for column in &profile.columns {
        emit.write(&ColumnRecord {
            kind: "pqbench.profile-column",
            column,
        })?;
    }
    for dependency in &profile.dependencies {
        emit.write(&DependencyRecord {
            kind: "pqbench.profile-dependency",
            dependency,
        })?;
    }
    emit.write(&EndRecord {
        kind: "pqbench.profile",
        event: "end",
        column_count: profile.columns.len(),
        num_rows: profile.num_rows,
        dependency_count: profile.dependencies.len(),
    })?;
    emit.finish(&summary(profile, args.output.as_deref()))
}

fn summary(profile: &Profile, output: Option<&Path>) -> String {
    let mut out = format!(
        "rows: {}\ncolumns: {}\n",
        profile.num_rows,
        profile.columns.len()
    );
    if !profile.dependencies.is_empty() {
        out.push_str(&format!("dependencies: {}\n", profile.dependencies.len()));
    }
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
    capabilities: &'a [pqbench::profile::Capability],
}

#[derive(Serialize)]
struct ColumnRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    column: &'a pqbench::profile::ColumnProfile,
}

#[derive(Serialize)]
struct DependencyRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    dependency: &'a pqbench::profile::DependencyProfile,
}

#[derive(Serialize)]
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    column_count: usize,
    num_rows: u64,
    dependency_count: usize,
}
