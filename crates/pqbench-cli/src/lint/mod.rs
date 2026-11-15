//! `list suggestions`: the storage a local blob's values do not need.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream;
use pqbench::{bytemass, v1};

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;

/// Arguments for `list suggestions`.
#[derive(Args)]
pub(crate) struct SuggestionsArgs {
    /// leading rows to inspect and price
    #[arg(long, default_value_t = pqbench::lint::SAMPLE_ROWS)]
    rows: usize,
}

/// Emit each table file's suggestions as its record arrives.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &SuggestionsArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::TableFile>("list suggestions").await?;
    let lake = lake.clone();
    let daemon = daemon.clone();
    let rows = args.rows;
    // One file at a time: decoding and pricing a sample is CPU-bound, so
    // overlapping files buys nothing and holds more samples in memory.
    let records = flat_map(parents, 1, move |file: v1::TableFile| {
        let lake = lake.clone();
        let daemon = daemon.clone();
        async move {
            let blob = file
                .blob
                .as_deref()
                .ok_or("a table-file record needs a blob to read")?;
            let (service, locator) =
                v1::name::parse_blob(blob).ok_or_else(|| format!("not a blob name: {blob}"))?;
            if service != "file" {
                return Err(format!(
                    "list suggestions reads local blobs; {blob} is not a local file"
                )
                .into());
            }
            let table = v1::name::parse_table_file(&file.name)
                .map(|(table, _)| table)
                .ok_or("a table-file record needs a catalogs/…/tables/… name")?;
            let env = daemon.resolve_env(&table).await?;
            let credentials = v1::Credentials::from_env(&env);
            let lake = lake.with_lease(Some(&credentials));
            let measured = bytemass::measure_file(&lake, blob).await?;
            let mut columns = measured.columns;
            for column in &mut columns {
                column.table = Some(table.clone());
                column.sample_rate = file.sample_rate;
                column.size_bytes = measured.blob.size_bytes;
            }
            let path = PathBuf::from(locator);
            let suggestions = pqbench::lint::suggestions(&path, blob, &columns, Some(rows))?;
            Ok(stream::iter(suggestions.into_iter().map(Ok::<_, CliError>)))
        }
    });
    drive(records).await
}
