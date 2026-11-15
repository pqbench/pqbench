//! `list columns`: read a Parquet footer and emit per-column byte masses.

use clap::Args;
use futures_util::stream;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;

/// Arguments for `list columns`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// files in flight at once
    #[arg(long, default_value_t = 128)]
    fan_out: usize,
}

/// List each table file's columns as its record arrives.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &BytemassArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::TableFile>("list columns").await?;
    let lake = lake.clone();
    let daemon = daemon.clone();
    let rows = flat_map(parents, args.fan_out, move |file: v1::TableFile| {
        let lake = lake.clone();
        let daemon = daemon.clone();
        async move {
            let measured = crate::measure::table_file(&lake, &daemon, &file).await?;
            Ok(stream::iter(
                rows(measured.blob, measured.columns)
                    .into_iter()
                    .map(Ok::<_, CliError>),
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
    Column(Box<v1::Column>),
}

/// The records one measured file writes, in order.
fn rows(blob: v1::Blob, columns: Vec<v1::Column>) -> Vec<Out> {
    let mut rows = Vec::with_capacity(1 + columns.len());
    rows.push(Out::Blob(blob));
    rows.extend(
        columns
            .into_iter()
            .map(|column| Out::Column(Box::new(column))),
    );
    rows
}
