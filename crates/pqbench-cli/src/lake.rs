use std::io::IsTerminal;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use clap::Args;
use pqbench::lake::{self, Lake};

use crate::document::{self, Record};
use crate::CliError;

/// Arguments for `lake`.
#[derive(Args)]
pub(crate) struct LakeArgs {
    /// lake directory, a `pqbench.lake` document, or `-` for standard input
    input: Option<String>,
    /// zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// catalogs, schemas, or tables to list at once
    #[arg(long, default_value = "4", value_name = "N")]
    concurrency: NonZeroUsize,
    /// keep names that match a glob or an exact prefix (`catalog`, `catalog.schema`)
    #[arg(long = "include", value_name = "PATTERN")]
    include: Vec<String>,
    /// drop names that match a glob or an exact prefix
    #[arg(long = "exclude", value_name = "PATTERN")]
    exclude: Vec<String>,
}

pub(crate) fn run(args: &LakeArgs) -> Result<(), CliError> {
    let _ = (args.output.as_ref(), args.concurrency);
    let mut lake = match &args.input {
        None if !std::io::stdin().is_terminal() => read_document("-")?,
        None => return Err("lake needs a directory or a document on standard input".into()),
        Some(value) if document::looks_like_document(value) => read_document(value)?,
        Some(path) => lake::discover(Path::new(path))?,
    };
    lake.tables
        .retain(|table| selected(&table.name, &args.include, &args.exclude));
    if lake.tables.is_empty() {
        return Err("lake listed no tables after include/exclude".into());
    }
    write(&lake)
}

fn read_document(input: &str) -> Result<Lake, CliError> {
    let mut lake = None;
    let reader: Box<dyn std::io::Read> = if input == "-" {
        Box::new(std::io::stdin())
    } else {
        document::open_file(Path::new(input))?
    };
    document::visit_records(reader, |record| match record {
        Record::Lake(listed) => {
            lake = Some(listed);
            Ok(())
        }
        Record::LakeSource(source) => {
            lake = Some(crate::unity::list_tables(&source)?);
            Ok(())
        }
        Record::Table(_)
        | Record::TableRef(_)
        | Record::RemoteSource(_)
        | Record::Begin(_)
        | Record::Log { .. }
        | Record::File { .. }
        | Record::End { .. } => Err(
            "pqbench lake reads a directory, a pqbench.lake document, or a pqbench.lake-source"
                .into(),
        ),
    })?;
    lake.ok_or_else(|| "empty lake document".into())
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

fn selected(name: &str, include: &[String], exclude: &[String]) -> bool {
    let kept = include.is_empty() || include.iter().any(|pattern| matches_name(name, pattern));
    kept && !exclude.iter().any(|pattern| matches_name(name, pattern))
}

fn matches_name(name: &str, pattern: &str) -> bool {
    if pattern.contains('*') || pattern.contains('?') || pattern.contains('[') {
        return glob_match(pattern, name);
    }
    name == pattern || name.starts_with(&format!("{pattern}.")) || name.starts_with(&format!("{pattern}/"))
}

fn glob_match(pattern: &str, name: &str) -> bool {
    let Ok(glob) = glob::Pattern::new(pattern) else {
        return name == pattern;
    };
    glob.matches(name)
}
