//! `diff`: compare two bytemass streams.

use crate::document::{self, Record};
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
        emit.write(delta)?;
    }
    emit.finish()
}

async fn read(lake: &pqbench::Lake, input: &str) -> Result<Vec<v1::Bytemass>, CliError> {
    if input != "-" && !document::is_document(input).await {
        return Ok(bytemass::bytemass(
            lake,
            &bytemass::BytemassRequest {
                inputs: vec![input.to_owned()],
                ..Default::default()
            },
        )
        .await?);
    }
    let mut columns = Vec::new();
    let mut records = document::records(input).await?;
    while let Some(record) = records.next().await {
        match record? {
            Record::Bytemass(column) => columns.push(column),
            Record::Blob(_) => {}
            _ => return Err("diff requires a bytemass stream or a Parquet file".into()),
        }
    }
    Ok(columns)
}
