use crate::document::{self, Record};
use crate::emit::{Align, Emitter, Format, Row};
use crate::CliError;
use clap::Args;
use futures_util::StreamExt;
use pqbench::{bytemass, diff};
use serde::Serialize;
use std::collections::BTreeSet;
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

pub(crate) async fn run(args: &DiffArgs) -> Result<(), CliError> {
    if args.left == "-" && args.right == "-" {
        return Err("only one diff input may be stdin".into());
    }
    let left = read(&args.left).await?;
    let right = read(&args.right).await?;
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

async fn read(input: &str) -> Result<Vec<bytemass::MassRow>, CliError> {
    if input != "-" && !document::is_document(input).await {
        return Ok(bytemass::bytemass(&bytemass::BytemassRequest {
            inputs: vec![input.to_owned()],
            ..Default::default()
        })
        .await?);
    }
    let mut rows = Vec::new();
    let mut tables = BTreeSet::new();
    let mut records = document::records(input).await?;
    while let Some(record) = records.next().await {
        match record? {
            Record::BytemassFile(_) => {}
            Record::BytemassRow { id, row } => {
                if !id.is_empty() && id != row.uri {
                    tables.insert(id);
                }
                rows.push(row);
            }
            _ => return Err("diff requires a bytemass stream or a Parquet file".into()),
        }
    }
    if tables.len() > 1 {
        return Err("diff requires one table per input; select a table before comparing".into());
    }
    Ok(rows)
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
