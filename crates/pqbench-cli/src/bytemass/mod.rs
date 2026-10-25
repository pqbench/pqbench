//! `bytemass`: read a Parquet footer and emit per-column byte masses.

use clap::Args;
use futures_util::stream;
use pqbench::bytemass;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// files in flight at once
    #[arg(long, default_value_t = 128)]
    fan_out: usize,
}

/// Measure each table file as its record arrives.
pub(crate) async fn run(lake: &pqbench::Lake, args: &BytemassArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::TableFile>("bytemass").await?;
    let lake = lake.clone();
    let rows = flat_map(parents, args.fan_out, move |file: v1::TableFile| {
        let lake = lake.clone();
        async move {
            let measured = bytemass::measure(&lake, &file).await?;
            Ok(stream::iter(
                measured_rows(measured).into_iter().map(Ok::<_, CliError>),
            ))
        }
    });
    drive(rows).await
}

/// One output record: a file's blob, then one row per column.
#[derive(serde::Serialize)]
#[serde(untagged)]
enum Out {
    Blob(v1::Blob),
    Column(v1::Bytemass),
}

/// The records one measured file writes, in order.
fn measured_rows(measured: bytemass::MeasuredFile) -> Vec<Out> {
    let mut rows = Vec::with_capacity(1 + measured.columns.len());
    rows.push(Out::Blob(measured.blob));
    rows.extend(measured.columns.into_iter().map(Out::Column));
    rows
}
