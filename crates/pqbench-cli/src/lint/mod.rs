//! `list suggestions`: the storage a local blob's values do not need.

use std::path::PathBuf;

use clap::Args;
use futures_util::stream;
use pqbench::v1;

use crate::parse;
use crate::source::{drive, flat_map};
use crate::CliError;

/// Arguments for `list suggestions`.
#[derive(Args)]
pub(crate) struct SuggestionsArgs {
    /// files in flight at once
    #[arg(long, default_value_t = 128)]
    fan_out: usize,
    /// leading rows to inspect and price
    #[arg(long, default_value_t = pqbench::lint::SAMPLE_ROWS)]
    rows: usize,
}

/// One output record: a suggestion or its file's total.
#[derive(serde::Serialize)]
#[serde(untagged)]
enum Out {
    Suggestion(pqbench::lint::Suggestion),
    Report(pqbench::lint::SuggestionReport),
}

/// Emit each table file's suggestions as its record arrives.
pub(crate) async fn run(
    _lake: &pqbench::Lake,
    _daemon: &crate::daemon::Daemon,
    args: &SuggestionsArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::TableFile>("list suggestions").await?;
    let rows = args.rows;
    let records = flat_map(
        parents,
        args.fan_out,
        move |file: v1::TableFile| async move {
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
            let path = PathBuf::from(locator);
            let linted = pqbench::lint::suggestions(&path, blob, Some(rows))?;
            let mut out: Vec<Result<Out, CliError>> =
                Vec::with_capacity(1 + linted.suggestions.len());
            out.extend(
                linted
                    .suggestions
                    .into_iter()
                    .map(|suggestion| Ok(Out::Suggestion(suggestion))),
            );
            out.push(Ok(Out::Report(linted.report)));
            Ok(stream::iter(out))
        },
    );
    drive(records).await
}
