//! `list files`: the files each partition's commits added.

use clap::Args;
use futures_util::StreamExt;
use pqbench::v1;

use crate::source;
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
    let rows = source::flat_map(
        parents,
        args.fan_out,
        |partition: v1::Partition| async move {
            let uri = partition
                .uri
                .clone()
                .ok_or("a partition record needs a table storage root")?;
            let lease = partition.credentials.filter(|lease| !lease.is_empty());
            let env = lease
                .as_ref()
                .map(v1::Credentials::to_env)
                .unwrap_or_default();
            let found = pqbench::file::list::list(
                lake,
                &table_name(&partition.name)?,
                &uri,
                &env,
                &partition.commit_versions,
            )
            .await
            .map_err(CliError::from)?;
            let min = args.min_file_size;
            Ok(found.filter_map(move |file| {
                let lease = lease.clone();
                async move {
                    match file {
                        Ok(mut file) => {
                            file.credentials = lease;
                            meets_min_file_size(&file, min).then_some(Ok(file))
                        }
                        Err(error) => Some(Err(error)),
                    }
                }
            }))
        },
    );
    source::drive(rows).await
}

/// The table a partition's name names: the read that names its files takes the
/// table resource, not the window.
fn table_name(name: &str) -> Result<String, CliError> {
    let (catalog, schema, table, _) = v1::name::parse_partition(name)
        .ok_or("a partition record needs a `/tables/{table}/partitions/{partition}` name")?;
    Ok(v1::name::table(&catalog, &schema, &table))
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
