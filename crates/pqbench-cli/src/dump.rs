use std::collections::BTreeMap;
use std::io::{IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

use clap::Args;
use pqbench::dump::{self, DumpFile, DumpRequest, RowGroups};
use pqbench::lake::Lake;
use pqbench::pattern::{self, Sample};
use pqbench::table::{TableFile, TableInfo};

use crate::document::{self, Record};
use crate::CliError;

/// Arguments for `dump`.
#[derive(Args)]
pub(crate) struct DumpArgs {
    /// parquet paths, a table or lake document, or `-` for standard input
    inputs: Vec<String>,
    /// write Parquet to this path instead of standard output
    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,
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
    let sample = Sample::parse(&args.sample)?;
    pattern::keep("", &args.include, &args.exclude)?;
    let _ = RowGroups::parse(&args.row_groups)?;
    let files = match resolve(args)? {
        Input::Parquet(inputs, env) => parquet_files(inputs, sample, args, env)?,
        Input::Files(files) => selected_files(files, sample, args)?,
        Input::Table(info) => table_files(&info, sample, args, None)?,
        Input::Lake(lake) => lake_files(lake, sample, args)?,
    };
    if args.output.is_none() && std::io::stdout().is_terminal() {
        return Err("dump writes Parquet; redirect standard output or pass --output".into());
    }
    let request = DumpRequest {
        files,
        row_groups: RowGroups::parse(&args.row_groups)?,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let bytes = runtime.block_on(dump::write_parquet(&request))?;
    write_bytes(args, &bytes)
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

#[allow(clippy::large_enum_variant)]
enum Input {
    Parquet(Vec<String>, BTreeMap<String, String>),
    Files(Vec<DumpFile>),
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
    if args.inputs.len() == 1 && document::looks_like_document(&args.inputs[0]) {
        return from_document(&args.inputs[0]);
    }
    Ok(Input::Parquet(args.inputs.clone(), BTreeMap::new()))
}

fn from_document(input: &str) -> Result<Input, CliError> {
    let reader: Box<dyn Read> = if input == "-" {
        Box::new(std::io::stdin())
    } else {
        document::open_file(Path::new(input))?
    };
    let mut table = None;
    let mut lake = None;
    let mut remote = None;
    let mut files = Vec::new();
    let mut envs = BTreeMap::new();
    document::visit_records(reader, |record| {
        match record {
            Record::Table(info) => table = Some(info),
            Record::Lake(listed) => lake = Some(listed),
            Record::RemoteSource(source) => remote = Some(source),
            Record::Begin(begin) => {
                envs.insert(begin.id.clone(), begin.env);
            }
            Record::File { id, file } => files.push(DumpFile {
                path: file.path,
                uri: file.uri,
                table: Some(id.clone()),
                env: envs.get(&id).cloned().unwrap_or_default(),
            }),
            Record::Log { .. } | Record::End { .. } | Record::LakeBegin | Record::LakeEnd => {}
            Record::TableRef(_) => {
                return Err("a table-ref names a table; pass it to `pqbench table` first".into());
            }
            Record::LakeSource(_) => {
                return Err("a lake source lists tables; pass it to `pqbench lake` first".into());
            }
            Record::BytemassBegin
            | Record::BytemassFile(_)
            | Record::BytemassRow { .. }
            | Record::BytemassEnd => {
                return Err("a bytemass stream goes to `pqbench viz`".into());
            }
        }
        Ok(())
    })?;
    if let Some(info) = table {
        return Ok(Input::Table(info));
    }
    if let Some(listed) = lake {
        return Ok(Input::Lake(listed));
    }
    if let Some(source) = remote {
        return Ok(Input::Parquet(source.inputs, source.env));
    }
    if files.is_empty() {
        return Err("document has no files to dump".into());
    }
    Ok(Input::Files(files))
}

fn lake_files(lake: Lake, sample: Sample, args: &DumpArgs) -> Result<Vec<DumpFile>, CliError> {
    let mut files = Vec::new();
    for table in &lake.tables {
        let info = table.info.as_ref().ok_or_else(|| {
            format!(
                "table {} has no log; pipe the lake through `pqbench table` first",
                table.name
            )
        })?;
        files.extend(table_files(info, sample, args, Some(table.name.as_str()))?);
    }
    if files.is_empty() {
        return Err("lake has no files to dump".into());
    }
    Ok(files)
}

fn parquet_files(
    inputs: Vec<String>,
    sample: Sample,
    args: &DumpArgs,
    env: BTreeMap<String, String>,
) -> Result<Vec<DumpFile>, CliError> {
    let inputs = if selecting(args) {
        pattern::select(inputs, String::as_str, &args.include, &args.exclude, sample)?
    } else {
        inputs
    };
    Ok(inputs
        .into_iter()
        .map(|input| DumpFile {
            path: input.clone(),
            uri: input,
            table: None,
            env: env.clone(),
        })
        .collect())
}

fn selected_files(
    files: Vec<DumpFile>,
    sample: Sample,
    args: &DumpArgs,
) -> Result<Vec<DumpFile>, CliError> {
    if files.is_empty() {
        return Err("document has no files to dump".into());
    }
    if selecting(args) {
        return Ok(pattern::select(
            files,
            |file| file.path.as_str(),
            &args.include,
            &args.exclude,
            sample,
        )?);
    }
    Ok(files)
}

fn table_files(
    info: &TableInfo,
    sample: Sample,
    args: &DumpArgs,
    table: Option<&str>,
) -> Result<Vec<DumpFile>, CliError> {
    if info.files.is_empty() {
        if !args.include.is_empty() {
            return Err("no paths matched --include".into());
        }
        return Ok(Vec::new());
    }
    let files = if selecting(args) {
        pattern::select(
            info.files.clone(),
            |file| file.path.as_str(),
            &args.include,
            &args.exclude,
            sample,
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
            env: info.env.clone(),
        })
        .collect())
}

fn selecting(args: &DumpArgs) -> bool {
    !args.include.is_empty() || !args.exclude.is_empty() || args.sample != "all"
}
