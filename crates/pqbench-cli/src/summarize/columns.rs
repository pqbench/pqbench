//! `summarize columns`: fold a bytemass stream into one row per table column.

use clap::Args;
use futures_util::StreamExt;
use pqbench::v1;

use crate::document::{self, Record};
use crate::emit::Emitter;
use crate::stdin::is_stream;
use crate::CliError;

/// Arguments for `summarize columns`.
#[derive(Args)]
pub(crate) struct ColumnsArgs {
    /// bytemass NDJSON stream, or `-` for standard input
    input: Option<String>,
}

pub(crate) async fn run(args: &ColumnsArgs) -> Result<(), CliError> {
    let input = match &args.input {
        None if is_stream() => "-",
        None => return Err("summarize columns needs a bytemass stream on standard input".into()),
        Some(value) => value.as_str(),
    };
    let (columns, blobs) = collect(input).await?;
    let rows = pqbench::mass::summarize_columns(&columns, &blobs)?;
    let mut out = Emitter::open();
    for row in &rows {
        out.write(row).await?;
    }
    out.finish().await
}

async fn collect(input: &str) -> Result<(Vec<v1::Column>, Vec<v1::Blob>), CliError> {
    let mut columns = Vec::new();
    let mut blobs = Vec::new();
    let mut records = document::records(input).await?;
    while let Some(record) = records.next().await {
        match record? {
            Record::Blob(blob) => blobs.push(blob),
            Record::Column(column) => columns.push(column),
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
        }
    }
    if columns.is_empty() {
        return Err("bytemass stream has no columns".into());
    }
    Ok((columns, blobs))
}
