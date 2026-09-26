use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use pqbench::table::{self, LoadEvent, LoadRequest, TableInfo};

use crate::document::{self, Record};
use crate::emit::Emitter;
use crate::CliError;

/// Arguments for `table`.
#[derive(Args)]
pub(crate) struct TableArgs {
    /// table URI, a document file, or `-` for standard input
    input: Option<String>,
    /// snapshot version; defaults to the latest version
    #[arg(long)]
    version: Option<u64>,
    /// omit min/max/null maps (keep num_records and bytes_per_row)
    #[arg(long)]
    no_stats: bool,
    /// zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

pub(crate) async fn run(args: &TableArgs) -> Result<(), CliError> {
    match &args.input {
        None if !std::io::stdin().is_terminal() => stream("-", args).await,
        None => Err("table needs a URI or a document on standard input".into()),
        Some(value) => {
            // An Iceberg `.metadata.json` is JSON on disk but names a table,
            // not a pqbench document; keep it on the table path.
            let metadata_json = value.ends_with(".metadata.json");
            if !metadata_json && document::is_document(value).await {
                stream(value, args).await
            } else {
                load_path(value, args).await
            }
        }
    }
}

async fn load_path(uri: &str, args: &TableArgs) -> Result<(), CliError> {
    let request = load_request(uri.to_string(), BTreeMap::new(), args)?.with_collect_files(false);
    let mut emit = Emitter::open("table", args.output.as_deref())?;
    let mut files = 0usize;
    let mut bytes = 0u64;
    let info = table::visit_load(&request, |event| match event {
        LoadEvent::BEGIN { info } => document::write_table_begin(&mut emit, uri, info)
            .map_err(|error| table::Error::new(error.to_string())),
        LoadEvent::FILE { file } => {
            files += 1;
            bytes += file.size_bytes;
            document::write_table_file(&mut emit, uri, file)
                .map_err(|error| table::Error::new(error.to_string()))
        }
    })
    .await?;
    document::write_table_end(&mut emit, uri, &info.partitions)?;
    emit.finish(&summary(1, files, bytes, args.output.as_deref()))
}

async fn load_info(
    uri: String,
    env: BTreeMap<String, String>,
    args: &TableArgs,
) -> Result<TableInfo, CliError> {
    Ok(table::load(&load_request(uri, env, args)?).await?)
}

fn load_request(
    uri: String,
    env: BTreeMap<String, String>,
    args: &TableArgs,
) -> Result<LoadRequest, CliError> {
    Ok(LoadRequest::new(uri, args.version, env).with_file_stats(!args.no_stats))
}

async fn stream(input: &str, args: &TableArgs) -> Result<(), CliError> {
    let mut emit = Emitter::open("table", args.output.as_deref())?;
    let mut tables = 0usize;
    let mut files = 0usize;
    let mut bytes = 0u64;
    document::visit_input(input, async |record| {
        match record {
            Record::TableRef(table_ref) => {
                let info = load_info(table_ref.uri, table_ref.env, args).await?;
                add(&info, &mut tables, &mut files, &mut bytes);
                document::write_table_records(&mut emit, &table_ref.id, &info)?;
            }
            Record::RemoteSource(source) => {
                for uri in source.inputs {
                    let info = load_info(uri.clone(), source.env.clone(), args).await?;
                    add(&info, &mut tables, &mut files, &mut bytes);
                    document::write_table_records(&mut emit, &uri, &info)?;
                }
            }
            Record::Table(info) => {
                add(&info, &mut tables, &mut files, &mut bytes);
                document::write_table_records(&mut emit, &info.uri, &info)?;
            }
            Record::Lake(lake) => {
                for table in lake.tables {
                    let info = load_info(table.uri, table.env, args).await?;
                    add(&info, &mut tables, &mut files, &mut bytes);
                    document::write_table_records(&mut emit, &table.name, &info)?;
                }
            }
            Record::LakeSource(_) => {
                return Err("a lake source lists tables; pass it to `pqbench lake` first".into());
            }
            Record::LakeBegin | Record::LakeEnd => {}
            Record::Begin(_) | Record::Commit { .. } | Record::File { .. } | Record::End { .. } => {
                return Err(
                    "a loaded table stream goes to `pqbench bytemass`, not `pqbench table`".into(),
                );
            }
            Record::BytemassBegin
            | Record::BytemassFile(_)
            | Record::BytemassRow { .. }
            | Record::BytemassEnd => {
                return Err("a bytemass stream goes to `pqbench viz`".into());
            }
        }
        Ok(())
    })
    .await?;
    emit.finish(&summary(tables, files, bytes, args.output.as_deref()))
}

fn add(info: &TableInfo, tables: &mut usize, files: &mut usize, bytes: &mut u64) {
    *tables += 1;
    *files += info.files.len();
    *bytes += file_bytes(info);
}

fn file_bytes(info: &TableInfo) -> u64 {
    info.files.iter().map(|file| file.size_bytes).sum()
}

fn summary(tables: usize, files: usize, bytes: u64, output: Option<&std::path::Path>) -> String {
    let mut out = format!("tables: {tables}\nfiles: {files} ({bytes} bytes)\n");
    if let Some(path) = output {
        out.push_str(&format!("output: {}\n", path.display()));
    }
    out
}
