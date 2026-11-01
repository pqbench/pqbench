//! `list columns`: read a Parquet footer and emit per-column byte masses.

use clap::Args;
use futures_util::stream;
use pqbench::{bytemass, v1};

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
pub(crate) async fn run(lake: &pqbench::Lake, args: &BytemassArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::TableFile>("list columns").await?;
    let lake = lake.clone();
    let rows = flat_map(parents, args.fan_out, move |file: v1::TableFile| {
        let lake = lake.clone();
        async move {
            let name = file
                .blob
                .as_deref()
                .ok_or_else(|| "a table-file record needs a blob to read".to_string())?;
            let lake = lake.with_lease(file.credentials.as_ref());
            let measured = bytemass::measure_file(&lake, name).await?;
            let mut blob = measured.blob;
            let mut columns = measured.columns;
            if let Some(rate) = file.sample_rate {
                if !(rate > 0.0 && rate <= 1.0) {
                    return Err(format!(
                        "a table file's sample rate must be in (0, 1], not {rate}"
                    )
                    .into());
                }
            }
            blob.sample_rate = file.sample_rate;
            let table = v1::name::parse_table_file(&file.name)
                .map(|(table, _)| table)
                .ok_or_else(|| {
                    "a table-file record needs a catalogs/…/tables/… name".to_string()
                })?;
            for column in &mut columns {
                column.table = Some(table.clone());
                column.sample_rate = file.sample_rate;
                column.size_bytes = blob.size_bytes;
                column.storage_class = blob.storage_class.clone();
            }
            Ok(stream::iter(
                rows(blob, columns).into_iter().map(Ok::<_, CliError>),
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
