use std::io::IsTerminal;

use clap::Args;
use pqbench::bytemass;
use pqbench::lake::Lake;
use pqbench::table::TableInfo;

use crate::document::{self, Document};
use crate::CliError;

/// Arguments for `bytemass`.
#[derive(Args)]
pub(crate) struct BytemassArgs {
    /// parquet paths, a table or lake document, or `-` for standard input
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
        Input::Parquet(inputs) => measure(inputs, args),
        Input::Table(info) => measure_table(info, args),
        Input::Lake(lake) => measure_lake(lake, args),
    }
}

enum Input {
    Parquet(Vec<String>),
    Table(TableInfo),
    Lake(Lake),
}

fn resolve(args: &BytemassArgs) -> Result<Input, CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err("bytemass needs parquet files or a table or lake document".into());
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
        Document::Lake(lake) => Ok(Input::Lake(lake)),
        Document::LakeSource(_) => {
            Err("a lake source lists tables; pass it to `pqbench lake` first".into())
        }
        Document::RemoteSource(source) => Ok(Input::Parquet(source.inputs)),
    }
}

fn measure_lake(lake: Lake, args: &BytemassArgs) -> Result<(), CliError> {
    if args.d3 {
        return Err(
            "a lake has more than one table; pass one pqbench.table document to --d3".into(),
        );
    }
    let mut reports = Vec::new();
    for table in &lake.tables {
        let info = table.info.as_ref().ok_or_else(|| {
            format!(
                "table {} has no log; pipe the lake through `pqbench table` first",
                table.name
            )
        })?;
        document::apply_env(&info.env)?;
        let inputs: Vec<String> = info.files.iter().map(|file| file.uri.clone()).collect();
        let rows = if inputs.is_empty() {
            Vec::new()
        } else {
            read_rows(inputs)?
        };
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
        reports.push((table.name.as_str(), rows));
    }
    if args.json {
        let tables = reports
            .iter()
            .map(|(name, rows)| {
                let summary = bytemass::aggregate(rows)?;
                Ok(serde_json::json!({
                    "name": name,
                    "file_count": summary.file_count,
                    "num_rows": summary.num_rows,
                    "columns": summary.columns,
                }))
            })
            .collect::<Result<Vec<_>, CliError>>()?;
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "pqbench.lake-report",
                "version": 1,
                "name": lake.name,
                "tables": tables,
            }))?
        );
        return Ok(());
    }
    for (name, rows) in &reports {
        println!("table: {name}");
        print!("{}", bytemass::render_text(rows)?);
    }
    Ok(())
}

fn measure_table(info: TableInfo, args: &BytemassArgs) -> Result<(), CliError> {
    document::apply_env(&info.env)?;
    let inputs: Vec<String> = info.files.iter().map(|file| file.uri.clone()).collect();
    if inputs.is_empty() {
        return render(&[], args);
    }
    let rows = read_rows(inputs)?;
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

fn measure(inputs: Vec<String>, args: &BytemassArgs) -> Result<(), CliError> {
    render(&read_rows(inputs)?, args)
}

fn read_rows(inputs: Vec<String>) -> Result<Vec<bytemass::MassRow>, CliError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    Ok(runtime.block_on(bytemass::bytemass(&bytemass::BytemassRequest { inputs }))?)
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
