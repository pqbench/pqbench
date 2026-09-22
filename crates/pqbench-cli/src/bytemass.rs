use std::collections::BTreeMap;
use std::io::IsTerminal;

use clap::Args;
use pqbench::bytemass;
use pqbench::table::TableInfo;

use crate::document::{self, Document};
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths, a `pqbench.table` document, or `-` for standard input
    inputs: Vec<String>,
    /// emit per-column byte masses as JSON instead of text stats
    #[arg(long = "json", conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML (open in a browser) instead of text stats
    #[arg(long = "d3")]
    d3: bool,
}

/// Build the typed request, measure, and render the CLI's chosen format. The
/// CLI owns the format decision; the library just returns the table.
pub(crate) fn run(args: &BytemassArgs) -> Result<(), CliError> {
    match resolve(args)? {
        Input::Parquet(inputs) => measure(inputs, BTreeMap::new(), args),
        Input::Remote { inputs, env } => measure(inputs, env, args),
        Input::Table(info) => measure_table(info, args),
    }
}

enum Input {
    Parquet(Vec<String>),
    Remote {
        inputs: Vec<String>,
        env: BTreeMap<String, String>,
    },
    Table(TableInfo),
}

fn resolve(args: &BytemassArgs) -> Result<Input, CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err("bytemass needs parquet files or a table document".into());
        }
        return from_document("-");
    }
    if args.inputs.len() == 1 && document::looks_like_json(&args.inputs[0]) {
        return from_document(&args.inputs[0]);
    }
    Ok(Input::Parquet(args.inputs.clone()))
}

fn from_document(input: &str) -> Result<Input, CliError> {
    match document::read_document(input)? {
        Document::Table(info) => Ok(Input::Table(info)),
        Document::RemoteSource(source) => Ok(Input::Remote {
            inputs: source.inputs,
            env: source.env,
        }),
    }
}

fn measure_table(info: TableInfo, args: &BytemassArgs) -> Result<(), CliError> {
    let inputs: Vec<String> = info.files.iter().map(|file| file.uri.clone()).collect();
    if inputs.is_empty() {
        return render(&[], args);
    }
    let rows = read_rows(inputs, info.env)?;
    for row in &rows {
        let file = info
            .files
            .iter()
            .find(|file| file.uri == row.file)
            .ok_or_else(|| format!("unexpected measured file: {}", row.file))?;
        if row.size != file.size {
            return Err(format!(
                "active file size differs from log: {} (expected {}, found {})",
                file.path, file.size, row.size
            )
            .into());
        }
    }
    render(&rows, args)
}

fn measure(
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
    args: &BytemassArgs,
) -> Result<(), CliError> {
    render(&read_rows(inputs, env)?, args)
}

fn read_rows(
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
) -> Result<Vec<bytemass::MassRow>, CliError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    Ok(
        runtime.block_on(bytemass::bytemass(&bytemass::BytemassRequest {
            inputs,
            env,
        }))?,
    )
}

fn render(rows: &[bytemass::MassRow], args: &BytemassArgs) -> Result<(), CliError> {
    let output = if args.json {
        bytemass::render_json(rows)?
    } else if args.d3 {
        bytemass::render_html(rows)?
    } else {
        bytemass::render_text(rows)?
    };
    print!("{output}");
    Ok(())
}
