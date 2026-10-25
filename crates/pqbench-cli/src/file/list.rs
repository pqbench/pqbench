//! `list files`: the files each partition's commits added.

use std::collections::BTreeMap;

use clap::Args;
use futures_util::stream::StreamExt;
use pqbench::v1;

use crate::emit::Emitter;
use crate::error::{context, fail_when_dropped};
use crate::source::expand;
use crate::CliError;

/// Arguments for `list files`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// partitions in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
    /// drop files whose logged size is below SIZE: bytes, or a number with a
    /// unit (kB/MB/GB/TB, KiB/MiB/GiB/TiB); e.g. 100MB
    #[arg(long, value_parser = parse_file_size, value_name = "SIZE")]
    min_file_size: Option<u64>,
}

/// List the files each partition's commits added: `list partitions | list files`.
///
/// Reads a partition stream, re-reads each table's log, and emits the files the
/// window's commits named — the **added in window** set, so a file a later
/// commit removes is still named. Each file is a [`v1::TableFile`] naming its
/// blob and carrying the partition's lease, so the storage stage reads it
/// without a catalog call.
pub(crate) async fn run(lake: &pqbench::Lake, args: &ListArgs) -> Result<(), CliError> {
    let parents = crate::parse::parents::<v1::Partition>("list files").await?;
    let mut emit = Emitter::open();
    let dropped = expand(
        parents,
        args.fan_out,
        &mut emit,
        |partition: v1::Partition| async move {
            partition_files(lake, partition, args.min_file_size).await
        },
    )
    .await?;
    emit.finish().await?;
    fail_when_dropped(dropped)
}

/// The files one partition's window added, each carrying the partition's lease.
/// A file whose read fails stays in the list as an `Err`, so its siblings are
/// still written; a partition that cannot be read at all fails on its own.
async fn partition_files(
    lake: &pqbench::Lake,
    partition: v1::Partition,
    min_file_size: Option<u64>,
) -> Result<Vec<Result<v1::TableFile, CliError>>, CliError> {
    let v1::Partition {
        name,
        commit_versions,
        uri,
        credentials,
        ..
    } = partition;
    let prepared = prepare_file_read(&name, uri, credentials)?;
    let mut found = pqbench::file::list::list(
        lake,
        &prepared.table,
        &prepared.uri,
        &prepared.env,
        &commit_versions,
    )
    .await
    .map_err(|error| context(&name, error))?;
    let mut files = Vec::new();
    while let Some(file) = found.next().await {
        match file {
            Ok(mut file) => {
                file.credentials = prepared.lease.clone();
                if meets_min_file_size(&file, min_file_size) {
                    files.push(Ok(file));
                }
            }
            Err(error) => files.push(Err(context(&name, error))),
        }
    }
    Ok(files)
}

/// The parts a partition's file read needs, after the record's own contract
/// checks: a bad name or a missing storage root fails that record, not the run.
struct FileRead {
    table: String,
    uri: String,
    lease: Option<v1::Credentials>,
    env: BTreeMap<String, String>,
}

fn prepare_file_read(
    name: &str,
    uri: Option<String>,
    credentials: Option<v1::Credentials>,
) -> Result<FileRead, CliError> {
    let (catalog, schema, table, _) =
        v1::name::parse_partition(name).ok_or_else(|| not_a_partition(name))?;
    let uri = uri.ok_or("a partition record needs a table storage root")?;
    let env = credentials
        .as_ref()
        .map(v1::Credentials::to_env)
        .unwrap_or_default();
    Ok(FileRead {
        table: v1::name::table(&catalog, &schema, &table),
        uri,
        lease: credentials,
        env,
    })
}

/// The error for a partition record whose name is not a partition resource.
fn not_a_partition(name: &str) -> CliError {
    format!("a partition name must be `…/tables/{{table}}/partitions/{{partition}}`: {name}").into()
}

/// Whether a file's logged size clears `--min-file-size`. A file whose log
/// records no size cannot be shown to be big, so the floor drops it.
fn meets_min_file_size(file: &v1::TableFile, min_file_size: Option<u64>) -> bool {
    min_file_size.is_none_or(|min| file.size_bytes.is_some_and(|size| size >= min))
}

/// Parse `--min-file-size`: a byte count, or a number with a unit.
///
/// `kB`/`MB`/`GB`/`TB` are powers of 1000 and `KiB`/`MiB`/`GiB`/`TiB` powers
/// of 1024 (case-insensitive); a bare number is bytes.
fn parse_file_size(value: &str) -> Result<u64, CliError> {
    let digits = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(digits);
    let count: u64 = number
        .parse()
        .map_err(|_| CliError::from(format!("--min-file-size needs a number: {value}")))?;
    if count == 0 {
        return Err(format!("--min-file-size must be positive: {value}").into());
    }
    let multiplier: u64 = match unit.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "tb" => 1_000_000_000_000,
        "kib" => 1_024,
        "mib" => 1_048_576,
        "gib" => 1_073_741_824,
        "tib" => 1_099_511_627_776,
        _ => {
            return Err(format!(
                "--min-file-size unit must be B, kB, MB, GB, TB, KiB, MiB, GiB, or TiB: {value}"
            )
            .into());
        }
    };
    count
        .checked_mul(multiplier)
        .ok_or_else(|| CliError::from(format!("--min-file-size is too large: {value}")))
}
