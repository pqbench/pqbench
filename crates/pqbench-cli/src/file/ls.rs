//! `file ls`: the files each partition's commits added.

use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use futures_util::stream::{self, StreamExt};
use pqbench::v1;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::emit::{Emitter, Format};
use crate::CliError;

/// Arguments for `file ls`.
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

/// List the files each partition's commits added: `partition ls | file ls`.
///
/// Reads a partition stream, re-reads each table's log, and emits the files the
/// window's commits named — the **added in window** set, so a file a later
/// commit removes is still named. Each file is a [`v1::TableFile`] naming its
/// blob and carrying the partition's lease, so the storage stage reads it
/// without a catalog call.
pub(super) async fn run(lake: &pqbench::Lake, args: &LsArgs) -> Result<(), CliError> {
    if std::io::stdin().is_terminal() {
        return Err("file ls reads partition records on standard input".into());
    }
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let lines = BufReader::new(tokio::io::stdin()).lines();
    let mut partitions = 0usize;
    let mut files = 0usize;
    let mut reads = partition_stream(lines)
        .map(|partition| async {
            let partition = partition?;
            let (catalog, schema, table, _) = v1::name::parse_partition(&partition.name)
                .ok_or_else(|| not_a_partition(&partition.name))?;
            let uri = partition
                .uri
                .clone()
                .ok_or("a partition record needs a table storage root")?;
            let env = partition
                .credentials
                .as_ref()
                .map(v1::Credentials::to_env)
                .unwrap_or_default();
            let found = pqbench::file::ls::list(lake, &uri, &env, &partition.commit_versions)
                .await
                .map_err(|error| CliError::from(error.to_string()))?;
            Ok::<_, CliError>((catalog, schema, table, partition.credentials, found))
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(result) = reads.next().await {
        let (catalog, schema, table, credentials, found) = result?;
        for file in &found {
            let table_file = v1::TableFile {
                name: v1::name::table_file(&catalog, &schema, &table, &file.path),
                blob: Some(v1::name::blob_name(&file.uri)),
                size_bytes: Some(file.size_bytes),
                credentials: credentials.clone(),
                ..v1::TableFile::default()
            };
            emit.write_row(&table_file).await?;
            files += 1;
        }
        partitions += 1;
    }
    emit.finish(&format!("partitions: {partitions}\nfiles: {files}\n"))
        .await
}

/// The error for a partition record whose name is not a partition resource.
fn not_a_partition(name: &str) -> CliError {
    format!("a partition name must be `…/tables/{{table}}/partitions/{{partition}}`: {name}").into()
}

/// Read partition records off a line stream, one at a time.
fn partition_stream(
    lines: tokio::io::Lines<BufReader<tokio::io::Stdin>>,
) -> impl stream::Stream<Item = Result<v1::Partition, CliError>> + Unpin {
    Box::pin(stream::try_unfold(lines, |mut lines| async move {
        loop {
            let Some(line) = lines.next_line().await? else {
                return Ok(None);
            };
            if line.trim().is_empty() {
                continue;
            }
            let partition: v1::Partition = serde_json::from_str(&line).map_err(|error| {
                CliError::from(format!("file ls reads partition NDJSON: {error}"))
            })?;
            if partition.uri.is_none() {
                return Err("a partition record needs a table storage root".into());
            }
            return Ok(Some((partition, lines)));
        }
    }))
}
