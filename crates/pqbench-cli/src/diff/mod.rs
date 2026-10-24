//! `diff`: compare two bytemass streams.

use crate::document::{self, Record};
use crate::emit::{Align, Emitter, Format, Row};
use crate::CliError;
use clap::Args;
use futures_util::StreamExt;
use pqbench::{bytemass, diff, v1};
use serde::Serialize;
use std::path::PathBuf;

/// Compare compressed column bytes from Parquet files or bytemass streams.
#[derive(Args)]
pub(crate) struct DiffArgs {
    left: String,
    right: String,
    /// Roll up dotted physical column paths to N components (positive).
    #[arg(long)]
    depth: Option<usize>,
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// Also write the lz4 NDJSON stream.
    #[arg(short, long)]
    output: Option<PathBuf>,
}

pub(crate) async fn run(lake: &pqbench::Lake, args: &DiffArgs) -> Result<(), CliError> {
    if args.left == "-" && args.right == "-" {
        return Err("only one diff input may be stdin".into());
    }
    let left = read(lake, &args.left).await?;
    let right = read(lake, &args.right).await?;
    let deltas = diff::compare(&left, &right, args.depth)?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    for delta in &deltas {
        emit.write_row(&DeltaRecord {
            kind: "pqbench.diff-column",
            delta,
        })
        .await?;
    }
    emit.finish(&format!("columns: {}\n", deltas.len())).await
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

#[derive(Serialize)]
struct DeltaRecord<'a> {
    kind: &'static str,
    #[serde(flatten)]
    delta: &'a diff::ColumnDelta,
}
impl Row for DeltaRecord<'_> {
    const HEADER: &'static [&'static str] = &[
        "column",
        "left_bytes",
        "right_bytes",
        "delta_bytes",
        "change_%",
    ];
    const ALIGN: &'static [Align] = &[
        Align::Left,
        Align::Right,
        Align::Right,
        Align::Right,
        Align::Right,
    ];
    fn cells(&self) -> Vec<String> {
        let value = self.delta;
        vec![
            value.column.clone(),
            value.left_bytes.map_or("-".into(), |n| n.to_string()),
            value.right_bytes.map_or("-".into(), |n| n.to_string()),
            value.delta_bytes.to_string(),
            value
                .change_percent
                .map_or("-".into(), |n| format!("{n:.2}")),
        ]
    }
}
