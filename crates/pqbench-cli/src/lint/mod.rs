//! `list suggestions` and `get suggestion`: the storage a local blob's values
//! do not need.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map, map};
use crate::CliError;

/// Arguments for `list suggestions`.
#[derive(Args)]
pub(crate) struct SuggestionsArgs {
    /// leading rows to inspect
    #[arg(long, default_value_t = pqbench::lint::SAMPLE_ROWS)]
    rows: usize,
}

/// Arguments for `get suggestion`.
#[derive(Args)]
pub(crate) struct VerifyArgs {
    /// leading rows to measure; defaults to the candidate's sample_row_count
    #[arg(long)]
    rows: Option<usize>,
}

/// Emit each table file's potential suggestions as its record arrives.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &SuggestionsArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::TableFile>("list suggestions").await?;
    let lake = lake.clone();
    let daemon = daemon.clone();
    let rows = args.rows;
    // One file at a time: decoding a sample is CPU-bound, so overlapping files
    // buys nothing and holds more samples in memory.
    let records = flat_map(parents, 1, move |file: v1::TableFile| {
        let lake = lake.clone();
        let daemon = daemon.clone();
        async move {
            let blob = file
                .blob
                .as_deref()
                .ok_or("a table-file record needs a blob to read")?;
            let path = local_path(blob)?;
            let measured = crate::measure::table_file(&lake, &daemon, &file).await?;
            let suggestions =
                pqbench::lint::suggestions(&path, &measured.blob, &measured.columns, Some(rows))?;
            Ok(stream::iter(suggestions.into_iter().map(Ok::<_, CliError>)))
        }
    });
    drive(records).await
}

/// Measure each potential suggestion and emit the same record enriched with
/// the control's verdict.
pub(crate) async fn verify(lake: &pqbench::Lake, args: &VerifyArgs) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Suggestion>("get suggestion").await?;
    let lake = lake.clone();
    let rows = args.rows;
    // One suggestion at a time: measuring re-encodes the sample, which is
    // CPU-bound, so overlap buys nothing.
    let records = map(parents, 1, move |candidate: v1::Suggestion| {
        let lake = lake.clone();
        async move {
            let (service, locator, column, rule) = v1::Suggestion::parse_name(&candidate.name)
                .ok_or_else(|| format!("not a suggestion name: {}", candidate.name))?;
            if candidate.column != column || candidate.rule != rule {
                return Err::<v1::Suggestion, CliError>(
                    format!(
                        "the suggestion name names `{column}` / `{rule}`, its fields say `{}` / `{}`",
                        candidate.column, candidate.rule
                    )
                    .into(),
                );
            }
            let blob = v1::name::blob(&service, &locator);
            let path = local_path(&blob)?;
            let measured = crate::measure::blob(&lake, &blob).await?;
            let column = measured
                .columns
                .iter()
                .find(|measured| {
                    v1::Column::parse_name(&measured.name)
                        .is_some_and(|(_, _, leaf)| leaf == candidate.column)
                })
                .ok_or_else(|| format!("the file has no column `{}`", candidate.column))?;
            Ok(pqbench::lint::verify(
                &path,
                &measured.blob,
                column,
                &candidate,
                rows,
            )?)
        }
    });
    drive(records).await
}

/// The local path a blob name points at; suggestions read local files only.
fn local_path(blob: &str) -> Result<PathBuf, CliError> {
    let (service, locator) =
        v1::name::parse_blob(blob).ok_or_else(|| format!("not a blob name: {blob}"))?;
    if service != "file" {
        return Err(format!("suggestions read local blobs; {blob} is not a local file").into());
    }
    Ok(PathBuf::from(locator))
}
