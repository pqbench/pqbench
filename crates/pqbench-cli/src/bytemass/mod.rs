//! `bytemass`: read a Parquet footer and emit per-column byte masses.

use clap::Args;
use futures_util::stream::{self, StreamExt};
use pqbench::bytemass;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// a Parquet path or storage URI; omit to read table-file records on
    /// standard input
    file: Option<String>,
    /// files in flight at once
    #[arg(long, default_value_t = 128)]
    fan_out: usize,
}

/// Measure each table file as its record arrives.
pub(crate) async fn run(lake: &pqbench::Lake, args: &BytemassArgs) -> Result<(), CliError> {
    let parents = match &args.file {
        Some(file) => stream::iter([Ok(table_file(file))]).boxed_local(),
        None => parse::parents::<v1::TableFile>("bytemass").await?,
    };
    let rows = flat_map(parents, args.fan_out, |file: v1::TableFile| async move {
        let measured = bytemass::measure(lake, &file).await?;
        Ok(stream::iter(
            measured_rows(measured).into_iter().map(Ok::<_, CliError>),
        ))
    });
    drive(rows).await
}

/// The table-file a path argument names, under the `local` catalog.
fn table_file(path: &str) -> v1::TableFile {
    let name = path
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(path);
    v1::TableFile {
        name: v1::name::table_file_name(&v1::name::local_table(path), name),
        blob: Some(v1::name::blob_name(path)),
        ..v1::TableFile::default()
    }
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
