//! `viz`: collect a bytemass stream into a static treemap page.

use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use clap::Args;
use futures_util::StreamExt;
use pqbench::v1;
use pqbench::viz;

use crate::document::{self, Record};
use crate::stdin::is_stream;
use crate::CliError;

/// Arguments for `viz`.
#[derive(Args)]
pub(crate) struct VizArgs {
    /// bytemass NDJSON stream, or `-` for standard input
    input: Option<String>,
    /// write `PREFIX.html` (required)
    #[arg(short = 'o', long = "output", value_name = "PREFIX")]
    output: Option<PathBuf>,
}

pub(crate) async fn run(args: &VizArgs) -> Result<(), CliError> {
    let input = match &args.input {
        None if is_stream() => "-",
        None => return Err("viz needs a bytemass stream on standard input".into()),
        Some(value) => value.as_str(),
    };
    let prefix = match &args.output {
        Some(path) => prefix(path),
        None => return Err("viz writes an html page; pass --output".into()),
    };
    let (columns, blobs) = collect(input).await?;
    viz::write_report(&prefix, &columns)?;
    if std::io::stdout().is_terminal() {
        print!("{}", summary(&prefix, &columns, &blobs));
    }
    Ok(())
}

async fn collect(input: &str) -> Result<(Vec<v1::Bytemass>, Vec<v1::Blob>), CliError> {
    let mut columns = Vec::new();
    let mut blobs = Vec::new();
    let mut records = document::records(input).await?;
    while let Some(record) = records.next().await {
        match record? {
            Record::Blob(blob) => blobs.push(blob),
            Record::Bytemass(column) => columns.push(column),
            Record::File(file) => {
                return Err(format!(
                    "viz reads a pqbench.bytemass stream, not `{}`; measure with `pqbench bytemass` first",
                    file.name
                )
                .into());
            }
        }
    }
    if columns.is_empty() {
        return Err("bytemass stream has no columns".into());
    }
    Ok((columns, blobs))
}

fn prefix(path: &Path) -> PathBuf {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("html" | "htm") => path.with_extension(""),
        _ => path.to_path_buf(),
    }
}

fn summary(prefix: &Path, columns: &[v1::Bytemass], blobs: &[v1::Blob]) -> String {
    format!(
        "files: {}\ncolumns: {}\noutput: {}\n",
        blobs.len(),
        columns.len(),
        prefix.with_extension("html").display()
    )
}
