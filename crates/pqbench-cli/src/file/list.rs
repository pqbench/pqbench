//! `list files`: a table's active files.

use clap::Args;
use futures_util::{future, StreamExt, TryStreamExt};
use pqbench::v1;

use crate::source::{drive, flat_map};
use crate::time::WindowArgs;
use crate::{parse, CliError};

/// Arguments for `list files`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// tables in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
    /// read only files the log dates in a window
    #[command(flatten)]
    window: WindowArgs,
    /// drop files whose logged size is below SIZE: bytes, or a number with a
    /// unit (kB/MB/GB/TB, KiB/MiB/GiB/TiB); e.g. 100MB
    #[arg(long, value_parser = parse_file_size, value_name = "SIZE")]
    min_file_size: Option<u64>,
}

/// List a table's active files: `get table | list files [--last 1d] | bytemass`.
///
/// Reads Table refs on standard input, resolves each one's snapshot, and writes
/// its active files as they arrive — the files active now, not the ones some
/// commit added. Each file carries the table's lease, so `bytemass` reads it
/// with no catalog call. Tables are walked `--fan-out` at a time and a file is
/// written as it is produced, so the run holds one log batch plus one file per
/// table in flight, however large the tables are.
pub(crate) async fn run(lake: &pqbench::Lake, args: &ListArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Table>("list files").await?;
    let window = args.window.window();
    let min = args.min_file_size;
    let lake = lake.clone();
    let rows = flat_map(parents, args.fan_out, move |table: v1::Table| {
        let lake = lake.clone();
        async move {
            let reference = table.name.clone();
            let files = pqbench::file::list::list(&lake, &table, window)
                .await
                .map_err(|error| crate::error::context(&reference, error))?;
            // Each table's own failures stay attributed to it, mid-walk too.
            Ok(files.map_err(move |error| crate::error::context(&reference, error)))
        }
    })
    .try_filter(move |file| future::ready(meets_min_file_size(file, min)))
    .boxed_local();
    drive(rows).await
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
