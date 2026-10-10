use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::{self, StreamExt};
use pqbench::table::TableFile;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::emit::{Align, Emitter, Format, Row};
use crate::CliError;

/// Arguments for `partition`: a partition's files.
#[derive(Args)]
pub(crate) struct PartitionArgs {
    #[command(subcommand)]
    command: PartitionCommand,
}

#[derive(Subcommand)]
pub(crate) enum PartitionCommand {
    /// List a partition's files — the ones its commits added
    Ls(LsArgs),
}

/// Arguments for `partition ls`.
#[derive(Args)]
pub(crate) struct LsArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// partitions in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(args: &PartitionArgs) -> Result<(), CliError> {
    match &args.command {
        PartitionCommand::Ls(ls) => run_ls(ls).await,
    }
}

/// List the files each partition's commits added: `table ls | partition ls`.
///
/// Reads a `pqbench.partition` stream, re-reads each table's log, and emits the
/// files the window's commits named — the **added in window** set, so a file a
/// later commit removes is still named. The env rides on every file, so a
/// short-lived lease travels with it; there is no begin/end envelope.
async fn run_ls(args: &LsArgs) -> Result<(), CliError> {
    if std::io::stdin().is_terminal() {
        return Err("partition ls reads pqbench.partition records on standard input".into());
    }
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let lines = BufReader::new(tokio::io::stdin()).lines();
    let mut partitions = 0usize;
    let mut files = 0usize;
    let mut reads = partition_stream(lines)
        .map(|partition| async {
            let partition = partition?;
            let versions: Vec<u64> = partition
                .commits
                .iter()
                .map(|commit| commit.version)
                .collect();
            let found = pqbench::partition::ls::list(&partition.table, &partition.env, &versions)
                .await
                .map_err(|error| CliError::from(error.to_string()))?;
            Ok::<_, CliError>((partition, found))
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(result) = reads.next().await {
        let (partition, found) = result?;
        for file in &found {
            emit.write_row(&FileRecord {
                kind: "pqbench.table-file",
                id: &partition.table,
                env: &partition.env,
                file,
            })
            .await?;
            files += 1;
        }
        partitions += 1;
    }
    emit.finish(&format!("partitions: {partitions}\nfiles: {files}\n"))
        .await
}

/// Read `pqbench.partition` records off a line stream, one at a time.
fn partition_stream(
    lines: tokio::io::Lines<BufReader<tokio::io::Stdin>>,
) -> impl stream::Stream<Item = Result<Partition, CliError>> + Unpin {
    Box::pin(stream::try_unfold(lines, |mut lines| async move {
        loop {
            let Some(line) = lines.next_line().await? else {
                return Ok(None);
            };
            if line.trim().is_empty() {
                continue;
            }
            let partition: Partition = serde_json::from_str(&line).map_err(|error| {
                CliError::from(format!(
                    "partition ls reads pqbench.partition NDJSON: {error}"
                ))
            })?;
            partition.validate()?;
            return Ok(Some((partition, lines)));
        }
    }))
}

/// One `pqbench.partition` record: a table, its env, and the window's commits.
#[derive(Deserialize)]
struct Partition {
    kind: String,
    version: u32,
    table: String,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    commits: Vec<Commit>,
}

#[derive(Deserialize)]
struct Commit {
    version: u64,
}

impl Partition {
    fn validate(&self) -> Result<(), CliError> {
        if self.kind != "pqbench.partition" {
            return Err(format!(
                "partition ls reads pqbench.partition records, found {:?}",
                self.kind
            )
            .into());
        }
        if self.version != 1 {
            return Err(
                "unsupported partition document; expected kind `pqbench.partition` version 1"
                    .into(),
            );
        }
        if self.table.is_empty() {
            return Err("a pqbench.partition record needs a table".into());
        }
        Ok(())
    }
}

/// One added file, with the env to read it: the shape `bytemass` measures.
#[derive(Serialize)]
struct FileRecord<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    env: &'a BTreeMap<String, String>,
    #[serde(flatten)]
    file: &'a TableFile,
}

impl Row for FileRecord<'_> {
    const HEADER: &'static [&'static str] = &["path", "size_bytes"];
    const ALIGN: &'static [Align] = &[Align::Left, Align::Right];

    fn cells(&self) -> Vec<String> {
        vec![self.file.path.clone(), self.file.size_bytes.to_string()]
    }
}
