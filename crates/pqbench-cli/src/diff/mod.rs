//! `diff`: compare two bytemass streams.

use crate::document::{self, Input, Record};
use crate::emit::Emitter;
use crate::CliError;
use clap::Args;
use futures_util::StreamExt;
use pqbench::{bytemass, diff, v1};

/// Compare compressed column bytes from Parquet files or bytemass streams.
#[derive(Args)]
pub(crate) struct DiffArgs {
    left: String,
    right: String,
    /// Roll up dotted physical column paths to N components (positive).
    #[arg(long)]
    depth: Option<usize>,
}

pub(crate) async fn run(lake: &pqbench::Lake, args: &DiffArgs) -> Result<(), CliError> {
    if args.left == "-" && args.right == "-" {
        return Err("only one diff input may be stdin".into());
    }
    let left = read(lake, &args.left).await?;
    let right = read(lake, &args.right).await?;
    let deltas = diff::compare(&left, &right, args.depth)?;
    let mut emit = Emitter::open();
    for delta in &deltas {
        emit.write(delta).await?;
    }
    emit.finish().await
}

async fn read(lake: &pqbench::Lake, input: &str) -> Result<Vec<v1::Column>, CliError> {
    let source = Input::parse(input);
    if let Input::File(path) = source {
        if !document::is_document(path).await {
            let blob = v1::name::blob_name(input);
            return Ok(bytemass::list_columns(lake, &blob).await?);
        }
    }
    let mut columns = Vec::new();
    let mut records = document::records(source).await?;
    while let Some(record) = records.next().await {
        match record? {
            Record::Column(column) => columns.push(column),
            Record::Blob(_) => {}
            _ => return Err("diff requires a column stream or a Parquet file".into()),
        }
    }
    Ok(columns)
}
