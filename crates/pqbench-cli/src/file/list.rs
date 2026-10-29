//! `list files`: a table's active files.

use clap::Args;
use futures_util::StreamExt;
use pqbench::v1;

use crate::emit::Emitter;
use crate::time::WindowArgs;
use crate::{parse, CliError};

/// Arguments for `list files`.
#[derive(Args)]
pub(crate) struct ListArgs {
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
/// with no catalog call. Files are written as they are produced, so the run
/// holds one log batch plus one file, however large the table.
pub(crate) async fn run(lake: &pqbench::Lake, args: &ListArgs) -> Result<(), CliError> {
    let mut parents = parse::parents::<v1::Table>("list files").await?;
    let window = args.window.window();
    let min = args.min_file_size;
    let mut emit = Emitter::open();
    let mut dropped = 0;
    let mut failure = None;
    while let Some(table) = parents.next().await {
        let table = match table {
            Ok(table) => table,
            Err(error) => {
                crate::error::report(&error);
                dropped += 1;
                continue;
            }
        };
        let walked = pqbench::file::list::visit(lake, &table, window, &mut |file| {
            if !meets_min_file_size(&file, min) {
                return true;
            }
            match emit.write(&file) {
                Ok(()) => true,
                Err(error) => {
                    failure = Some(error);
                    false
                }
            }
        })
        .await;
        match walked {
            Ok(_) => {}
            Err(error) => {
                crate::error::report(&error.into());
                dropped += 1;
            }
        }
        if let Some(error) = failure.take() {
            return Err(error);
        }
    }
    emit.finish()?;
    crate::error::fail_when_dropped(dropped)
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
