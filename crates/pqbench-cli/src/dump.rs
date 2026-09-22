use std::io::{IsTerminal, Write};
use std::path::PathBuf;

use clap::Args;
use pqbench::dump::{self, DumpFile, DumpRequest, RowGroups};
use pqbench::lake::Lake;
use pqbench::pattern::{self, Sample};
use pqbench::table::{TableFile, TableInfo};

use crate::document::{self, Document};
use crate::CliError;

/// Arguments for `dump`.
#[derive(Args)]
pub(crate) struct DumpArgs {
    /// parquet paths, a table or lake document, or `-` for standard input
    inputs: Vec<String>,
    /// write Parquet to this path instead of standard output
    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,
    /// emit CSV instead of Parquet
    #[arg(long, conflicts_with = "json")]
    csv: bool,
    /// emit NDJSON instead of Parquet
    #[arg(long = "json")]
    json: bool,
    /// keep files whose partition path matches this glob (repeatable)
    #[arg(long, value_name = "GLOB")]
    include: Vec<String>,
    /// drop files whose partition path matches this glob (repeatable)
    #[arg(long, value_name = "GLOB")]
    exclude: Vec<String>,
    /// file sample after include/exclude: all, every:N, first:N
    #[arg(long, value_name = "METHOD", default_value = "all")]
    sample: String,
    /// row groups to read from each file: all, first:N
    #[arg(long, value_name = "METHOD", default_value = "all")]
    row_groups: String,
}

pub(crate) fn run(args: &DumpArgs) -> Result<(), CliError> {
    let files = match resolve(args)? {
        Input::Parquet(inputs) => parquet_files(inputs, args)?,
        Input::Table(info) => {
            document::apply_env(&info.env)?;
            table_files(&info, args, None)?
        }
        Input::Lake(lake) => lake_files(lake, args)?,
    };
    let request = DumpRequest {
        files,
        row_groups: RowGroups::parse(&args.row_groups)?,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    if args.csv {
        let dump = runtime.block_on(dump::dump(&request))?;
        return write_text(args, dump::render_csv(&dump));
    }
    if args.json {
        let dump = runtime.block_on(dump::dump(&request))?;
        return write_text(args, dump::render_json(&dump)?);
    }
    if args.output.is_none() && std::io::stdout().is_terminal() {
        return Err("dump writes Parquet; redirect standard output or pass --output".into());
    }
    let bytes = runtime.block_on(dump::write_parquet(&request))?;
    write_bytes(args, &bytes)
}

fn write_text(args: &DumpArgs, text: String) -> Result<(), CliError> {
    match &args.output {
        Some(path) => std::fs::write(path, text)
            .map_err(|error| format!("cannot write {}: {error}", path.display()).into()),
        None => {
            print!("{text}");
            Ok(())
        }
    }
}

fn write_bytes(args: &DumpArgs, bytes: &[u8]) -> Result<(), CliError> {
    match &args.output {
        Some(path) => std::fs::write(path, bytes)
            .map_err(|error| format!("cannot write {}: {error}", path.display()).into()),
        None => {
            std::io::stdout().write_all(bytes)?;
            Ok(())
        }
    }
}

enum Input {
    Parquet(Vec<String>),
    Table(TableInfo),
    Lake(Lake),
}

fn resolve(args: &DumpArgs) -> Result<Input, CliError> {
    if args.inputs.is_empty() {
        if std::io::stdin().is_terminal() {
            return Err("dump needs parquet files or a table or lake document".into());
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

fn lake_files(lake: Lake, args: &DumpArgs) -> Result<Vec<DumpFile>, CliError> {
    let mut files = Vec::new();
    for table in &lake.tables {
        let info = table.info.as_ref().ok_or_else(|| {
            format!(
                "table {} has no log; pipe the lake through `pqbench table` first",
                table.name
            )
        })?;
        document::apply_env(&info.env)?;
        files.extend(table_files(info, args, Some(table.name.as_str()))?);
    }
    if files.is_empty() {
        return Err("lake has no files to dump".into());
    }
    Ok(files)
}

fn parquet_files(inputs: Vec<String>, args: &DumpArgs) -> Result<Vec<DumpFile>, CliError> {
    let inputs = if selecting(args) {
        pattern::select(
            inputs,
            String::as_str,
            &args.include,
            &args.exclude,
            sample(args)?,
        )?
    } else {
        inputs
    };
    Ok(inputs
        .into_iter()
        .map(|input| DumpFile {
            path: input.clone(),
            uri: input,
            table: None,
        })
        .collect())
}

fn table_files(
    info: &TableInfo,
    args: &DumpArgs,
    table: Option<&str>,
) -> Result<Vec<DumpFile>, CliError> {
    if info.files.is_empty() {
        return Ok(Vec::new());
    }
    let files = if selecting(args) {
        pattern::select(
            info.files.clone(),
            |file| file.path.as_str(),
            &args.include,
            &args.exclude,
            sample(args)?,
        )?
    } else {
        info.files.clone()
    };
    Ok(files
        .into_iter()
        .map(|file: TableFile| DumpFile {
            path: file.path,
            uri: file.uri,
            table: table.map(str::to_string),
        })
        .collect())
}

fn selecting(args: &DumpArgs) -> bool {
    !args.include.is_empty() || !args.exclude.is_empty() || args.sample != "all"
}

fn sample(args: &DumpArgs) -> Result<Sample, CliError> {
    Ok(Sample::parse(&args.sample)?)
}
