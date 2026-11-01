//! `summarize columns`: fold a bytemass stream into one row per table column.

use std::path::Path;

use clap::Args;
use futures_util::TryStreamExt;

use pqbench::mass;
use pqbench::v1;

use crate::document::{self, Input, Record};
use crate::emit::Emitter;
use crate::stdin::is_stream;
use crate::CliError;

/// Arguments for `summarize columns`.
#[derive(Args)]
pub(crate) struct ColumnsArgs {
    /// bytemass NDJSON stream; reads standard input when omitted
    input: Option<String>,
}

pub(crate) async fn run(args: &ColumnsArgs) -> Result<(), CliError> {
    let input = match &args.input {
        Some(value) => Input::File(Path::new(value)),
        None if is_stream() => Input::Stdin,
        None => return Err("summarize columns needs a bytemass stream on standard input".into()),
    };
    let columns = document::records(input).await?.try_filter_map(column);
    let rows = pqbench::mass::summarize_columns(columns)
        .await
        .map_err(|error| match error {
            mass::SummarizeError::Stream(error) => error,
            mass::SummarizeError::Summary(error) => error.into(),
            other => format!("summarize columns failed: {other}").into(),
        })?;
    if rows.is_empty() {
        return Err("bytemass stream has no columns".into());
    }
    let mut out = Emitter::open();
    for row in &rows {
        out.write(row).await?;
    }
    out.finish().await
}

/// The column rows of the stream; a blob is structural, not a measurement.
async fn column(record: Record) -> Result<Option<v1::Column>, CliError> {
    Ok(match record {
        Record::Column(column) => Some(column),
        Record::Blob => None,
        Record::File(file) => {
            return Err(format!(
                "summarize columns reads a `list columns` stream, not `{}`; measure with `pqbench list columns` first",
                file.name
            )
            .into());
        }
        Record::Mass(_) => {
            return Err("summarize columns reads a bytemass stream, not a summary".into());
        }
    })
}
