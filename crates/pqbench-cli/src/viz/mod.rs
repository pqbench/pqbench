//! `viz`: draw a `summarize columns` document as a static treemap page.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use clap::Args;
use futures_util::StreamExt;
use pqbench::mass::ColumnMass;
use pqbench::viz;

use crate::document::{self, Record};
use crate::stdin::is_stream;
use crate::CliError;

/// Arguments for `viz`.
#[derive(Args)]
pub(crate) struct VizArgs {
    /// summarize columns NDJSON stream, or `-` for standard input
    input: Option<String>,
    /// write `PREFIX.html` (required)
    #[arg(short = 'o', long = "output", value_name = "PREFIX")]
    output: Option<PathBuf>,
}

pub(crate) async fn run(args: &VizArgs) -> Result<(), CliError> {
    let input = match &args.input {
        None if is_stream() => "-",
        None => return Err("viz needs a summarize columns stream on standard input".into()),
        Some(value) => value.as_str(),
    };
    let prefix = match &args.output {
        Some(path) => prefix(path),
        None => return Err("viz writes an html page; pass --output".into()),
    };
    let rows = collect(input).await?;
    viz::write_report(&prefix, &rows)?;
    if std::io::stdout().is_terminal() {
        print!("{}", summary(&prefix, &rows));
    }
    Ok(())
}

async fn collect(input: &str) -> Result<Vec<ColumnMass>, CliError> {
    let mut rows = Vec::new();
    let mut records = document::records(input).await?;
    while let Some(record) = records.next().await {
        match record? {
            Record::Mass(row) => rows.push(row),
            Record::Blob(_) | Record::Column(_) => {
                return Err(
                    "viz reads a summarize columns stream; run `pqbench summarize columns` first"
                        .into(),
                );
            }
            Record::File(file) => {
                return Err(format!(
                    "viz reads a summarize columns stream, not `{}`; pipe through `pqbench bytemass` and `pqbench summarize columns` first",
                    file.name
                )
                .into());
            }
        }
    }
    if rows.is_empty() {
        return Err("summarize columns stream has no rows".into());
    }
    Ok(rows)
}

fn prefix(path: &Path) -> PathBuf {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("html" | "htm") => path.with_extension(""),
        _ => path.to_path_buf(),
    }
}

fn summary(prefix: &Path, rows: &[ColumnMass]) -> String {
    format!(
        "rows: {}\noutput: {}\n",
        rows.len(),
        prefix.with_extension("html").display()
    )
}
