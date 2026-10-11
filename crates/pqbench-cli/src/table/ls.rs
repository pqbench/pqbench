//! `table ls`: a table's natural partitions, grouped by commit time.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::table;
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
use crate::source::{self, read_input, ref_env, table_ref};
use crate::CliError;

/// Arguments for `table ls`.
#[derive(Args)]
pub(crate) struct LsArgs {
    /// table URI or a document file; without it, read table refs on stdin
    input: Option<String>,
    /// window width, epoch-aligned: e.g. 1h, 1d, 1w [env: PQB_TABLE_EVERY]
    #[arg(
        long,
        value_parser = parse_window,
        value_name = "WINDOW",
        env = "PQB_TABLE_EVERY",
        default_value = "1d"
    )]
    every: i64,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// tables in flight at once [env: PQB_TABLE_FAN_OUT]
    #[arg(long, default_value_t = 64, env = "PQB_TABLE_FAN_OUT")]
    fan_out: usize,
}

/// List a table's natural partitions: commits grouped into commit-time windows.
pub(super) async fn run_ls(lake: &pqbench::Lake, args: &LsArgs) -> Result<(), CliError> {
    let input = read_input("table ls").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let mut partitions = 0usize;
    match &args.input {
        Some(uri) if uri != "-" => {
            if input.first.is_some() {
                return Err(
                    "table ls takes a table URI or a pqbench.table-ref v2 stream, not both".into(),
                );
            }
            let env = BTreeMap::new();
            partitions += list_partitions(lake, uri, &env, args.every, &mut emit).await?;
        }
        _ => {
            if !input.piped {
                return Err("table ls needs a table URI or a pqbench.table-ref v2 stream".into());
            }
            let records = source::records("table ls", input.first, input.lines);
            let mut reads = records
                .map(|record| async {
                    let record = record?;
                    table_ref("table ls", &record)?;
                    let uri = record["storage_path"]
                        .as_str()
                        .filter(|path| !path.is_empty())
                        .ok_or_else(|| {
                            CliError::from(
                                "a table-ref has no storage path; run `pqbench table info` first",
                            )
                        })?
                        .to_string();
                    let env = ref_env(&record);
                    let found = table::ls::list(lake, &uri, &env, args.every)
                        .await
                        .map_err(|error| CliError::from(error.to_string()))?;
                    Ok::<_, CliError>((uri, env, found))
                })
                .buffer_unordered(args.fan_out.max(1));
            while let Some(result) = reads.next().await {
                let (uri, env, found) = result?;
                for partition in &found {
                    emit.write_row(&partition_record(&uri, partition, &env))
                        .await?;
                    partitions += 1;
                }
            }
        }
    }
    emit.finish(&format!("partitions: {partitions}\n")).await
}

/// Emit one table's partitions; returns how many were written.
async fn list_partitions(
    lake: &pqbench::Lake,
    uri: &str,
    env: &BTreeMap<String, String>,
    window: i64,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let found = table::ls::list(lake, uri, env, window)
        .await
        .map_err(|error| CliError::from(error.to_string()))?;
    let mut count = 0;
    for partition in &found {
        emit.write_row(&partition_record(uri, partition, env))
            .await?;
        count += 1;
    }
    Ok(count)
}

/// Parse a window width: a positive integer and a unit (`m`, `h`, `d`, `w`).
fn parse_window(value: &str) -> Result<i64, CliError> {
    let (number, unit) = value.split_at(
        value
            .len()
            .checked_sub(1)
            .ok_or_else(|| CliError::from("--every is empty"))?,
    );
    let count: i64 = number
        .parse()
        .map_err(|_| CliError::from(format!("--every `{value}` needs a number and a unit")))?;
    if count <= 0 {
        return Err(format!("--every must be positive: {value}").into());
    }
    let millis = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return Err(format!("--every unit must be m, h, d, or w: {value}").into()),
    };
    Ok(count * millis)
}

/// The document `table ls` writes, one line per partition.
#[derive(Serialize)]
struct PartitionRecord<'a> {
    kind: &'static str,
    version: u32,
    table: &'a str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    env: &'a BTreeMap<String, String>,
    definition: Definition,
    commits: Vec<CommitCell>,
}

/// A partition's definition: a natural commit-time window.
#[derive(Serialize)]
struct Definition {
    kind: &'static str,
    first_time: i64,
    last_time: i64,
}

/// One commit in a partition, in the emitted document.
#[derive(Serialize)]
struct CommitCell {
    version: u64,
    commit_time: i64,
}

impl Row for PartitionRecord<'_> {
    const HEADER: &'static [&'static str] = &["table", "first_time", "last_time", "commits"];
    const ALIGN: &'static [Align] = &[Align::Left, Align::Right, Align::Right, Align::Right];

    fn cells(&self) -> Vec<String> {
        vec![
            self.table.to_string(),
            self.definition.first_time.to_string(),
            self.definition.last_time.to_string(),
            self.commits.len().to_string(),
        ]
    }
}

fn partition_record<'a>(
    table: &'a str,
    partition: &table::ls::Partition,
    env: &'a BTreeMap<String, String>,
) -> PartitionRecord<'a> {
    PartitionRecord {
        kind: "pqbench.partition",
        version: 1,
        table,
        env,
        definition: Definition {
            kind: "natural",
            first_time: partition.first_time,
            last_time: partition.last_time,
        },
        commits: partition
            .commits
            .iter()
            .map(|commit| CommitCell {
                version: commit.version,
                commit_time: commit.commit_time,
            })
            .collect(),
    }
}
