use std::io::IsTerminal;
use std::path::Path;

use clap::Args;
use pqbench::lake::{self, Lake};

use crate::document::{self, Document};
use crate::CliError;

/// Arguments for `lake`.
#[derive(Args)]
pub(crate) struct LakeArgs {
    /// lake directory, a `pqbench.lake` document, or `-` for standard input
    input: Option<String>,
}

pub(crate) fn run(args: &LakeArgs) -> Result<(), CliError> {
    let lake = match &args.input {
        None if !std::io::stdin().is_terminal() => read_document("-")?,
        None => return Err("lake needs a directory or a document on standard input".into()),
        Some(value) if document::looks_like_json(value) => read_document(value)?,
        Some(path) => lake::discover(Path::new(path))?,
    };
    write(&lake)
}

fn read_document(input: &str) -> Result<Lake, CliError> {
    match document::read_document(input)? {
        Document::Lake(lake) => Ok(lake),
        Document::LakeSource(source) => crate::unity::list_tables(&source),
        Document::Table(_) | Document::RemoteSource(_) => Err(
            "pqbench lake reads a directory, a pqbench.lake document, or a pqbench.lake-source"
                .into(),
        ),
    }
}

pub(crate) fn write(lake: &Lake) -> Result<(), CliError> {
    let mut stdout = std::io::stdout().lock();
    use std::io::Write;
    if stdout.is_terminal() {
        write!(stdout, "{}", lake::render_text(lake))?;
    } else {
        writeln!(stdout, "{}", lake::render_json(lake)?)?;
    }
    Ok(())
}
