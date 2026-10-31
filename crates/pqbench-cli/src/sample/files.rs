//! `sample files`: keep a rate of a table's files, by size.

use std::cell::RefCell;
use std::rc::Rc;

use clap::Args;
use futures_util::StreamExt;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, Records};
use crate::CliError;

/// Arguments for `sample files`.
#[derive(Args)]
pub(crate) struct FilesArgs {
    /// keep probability at 256 MiB; 1 keeps every file
    #[arg(
        long,
        default_value = "1/100",
        value_name = "RATE",
        value_parser = parse_rate
    )]
    rate: f64,
}

/// Keep a sample of a table's files: `list files | sample files | bytemass`.
///
/// Reads table-file records on standard input and writes the kept ones, each
/// with `sample_rate` set, so a sum over the stream can weight them. The
/// decision is per record and needs only its name and size, so the stage
/// streams: it never holds the table.
pub(crate) async fn run(args: &FilesArgs) -> Result<(), CliError> {
    let rate = args.rate;
    let parents = parse::parents::<v1::TableFile>("sample files").await?;
    let summary = Rc::new(RefCell::new(Summary::default()));
    let counted = Rc::clone(&summary);
    let rows: Records<'_, v1::TableFile> = parents
        .filter_map(move |record| {
            let counted = Rc::clone(&counted);
            async move { keep(record, rate, &counted) }
        })
        .boxed_local();
    let outcome = drive(rows).await;
    let summary = summary.borrow();
    if summary.files > 0 {
        eprintln!("{}", summary.line());
    }
    outcome
}

/// Keep one record or drop it: the stage's whole decision, one record at a
/// time.
fn keep(
    record: Result<v1::TableFile, CliError>,
    rate: f64,
    summary: &RefCell<Summary>,
) -> Option<Result<v1::TableFile, CliError>> {
    let mut file = match record {
        Ok(file) => file,
        Err(error) => return Some(Err(error)),
    };
    summary.borrow_mut().observe(&file);
    match pqbench::file::sample::select(&file, rate) {
        Ok(Some(sample_rate)) => {
            file.sample_rate = Some(sample_rate);
            summary.borrow_mut().keep(&file);
            Some(Ok(file))
        }
        Ok(None) => None,
        Err(error) => Some(Err(error.into())),
    }
}

/// Parse `--rate`: a fraction (`1/100`) or a decimal (`0.01`).
fn parse_rate(text: &str) -> Result<f64, String> {
    let bad = || format!("bad rate `{text}`; expected `1/100` or `0.01`");
    let value = match text.split_once('/') {
        Some((numerator, denominator)) => {
            let numerator: f64 = numerator.trim().parse().map_err(|_| bad())?;
            let denominator: f64 = denominator.trim().parse().map_err(|_| bad())?;
            if denominator == 0.0 {
                return Err(format!("bad rate `{text}`; the denominator is zero"));
            }
            numerator / denominator
        }
        None => text.trim().parse().map_err(|_| bad())?,
    };
    if value > 0.0 && value <= 1.0 {
        Ok(value)
    } else {
        Err(format!("rate `{text}` is outside (0, 1]"))
    }
}

/// What the stage read and kept, for the coverage line.
#[derive(Default)]
struct Summary {
    files: u64,
    bytes: u64,
    kept: u64,
    kept_bytes: u64,
}

impl Summary {
    /// Count one record read.
    fn observe(&mut self, file: &v1::TableFile) {
        self.files += 1;
        self.bytes = self.bytes.saturating_add(file.size_bytes.unwrap_or(0));
    }

    /// Count one record kept.
    fn keep(&mut self, file: &v1::TableFile) {
        self.kept += 1;
        self.kept_bytes = self.kept_bytes.saturating_add(file.size_bytes.unwrap_or(0));
    }

    /// The coverage line, on standard error.
    fn line(&self) -> String {
        let percent = if self.bytes == 0 {
            0.0
        } else {
            100.0 * self.kept_bytes as f64 / self.bytes as f64
        };
        format!(
            "sampled {} of {} files, {} of {} ({percent:.1}%)",
            self.kept,
            self.files,
            human_bytes(self.kept_bytes),
            human_bytes(self.bytes)
        )
    }
}

/// A byte count in the largest unit that keeps it above one.
fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}
