use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use pqbench::table::{self, LoadRequest, TableInfo};

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
    /// zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

pub(crate) async fn run(args: &TableArgs) -> Result<(), CliError> {
    match &args.input {
        None if !std::io::stdin().is_terminal() => stream("-", args).await,
        None => Err("table needs a URI or a document on standard input".into()),
        Some(value) => {
            if document::is_document(value).await {
                stream(value, args).await
            } else {
                load_path(value, args).await
            }
        }
    }
}

async fn load_path(uri: &str, args: &TableArgs) -> Result<(), CliError> {
    let info = load_info(uri.to_string(), BTreeMap::new(), args.version).await?;
    let mut emit = Emitter::open("table", args.output.as_deref())?;
    document::write_table_records(&mut emit, uri, &info)?;
    emit.finish(&summary(
        1,
        info.files.len(),
        file_bytes(&info),
        args.output.as_deref(),
    ))
}

async fn load_info(
    uri: String,
    env: BTreeMap<String, String>,
    version: Option<u64>,
) -> Result<TableInfo, CliError> {
    Ok(table::load(&LoadRequest::new(uri, version, env)).await?)
}

async fn stream(input: &str, args: &TableArgs) -> Result<(), CliError> {
    let mut emit = Emitter::open("table", args.output.as_deref())?;
    let mut tables = 0usize;
    let mut files = 0usize;
    let mut bytes = 0u64;
    document::visit_input(input, async |record| {
        match record {
            Record::TableRef(table_ref) => {
                let info = load_info(table_ref.uri, table_ref.env, args.version).await?;
                add(&info, &mut tables, &mut files, &mut bytes);
                document::write_table_records(&mut emit, &table_ref.id, &info)?;
            }
            Record::RemoteSource(source) => {
                for uri in source.inputs {
                    let info = load_info(uri.clone(), source.env.clone(), args.version).await?;
                    add(&info, &mut tables, &mut files, &mut bytes);
                    document::write_table_records(&mut emit, &uri, &info)?;
                }
            }
            Record::Table(info) => {
                add(&info, &mut tables, &mut files, &mut bytes);
                document::write_table_records(&mut emit, &info.uri, &info)?;
            }
            Record::Begin(_) | Record::Commit { .. } | Record::File { .. } | Record::End { .. } => {
                return Err(
                    "a loaded table stream goes to `pqbench bytemass`, not `pqbench table`".into(),
                );
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
