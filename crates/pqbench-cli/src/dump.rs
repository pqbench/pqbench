use std::collections::{BTreeMap, BTreeSet};
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use pqbench::dump::{self, DumpFile};
use pqbench::table::{self, LoadRequest, TableInfo};

use crate::document::{self, Record};
use crate::CliError;

/// Arguments for `dump`.
#[derive(Args)]
pub(crate) struct DumpArgs {
    /// directory to write the Parquet files into
    output: PathBuf,
    /// table URI, a document file, or `-` for standard input
    input: Option<String>,
}

pub(crate) async fn run(args: &DumpArgs) -> Result<(), CliError> {
    let entries = resolve(args).await?;
    // One table keeps its own layout; a lake nests each table under its name so
    // files with the same relative path do not collide.
    let multiple = entries
        .iter()
        .map(|(id, _)| id)
        .collect::<BTreeSet<_>>()
        .len()
        > 1;
    let files: Vec<DumpFile> = entries
        .into_iter()
        .map(|(id, mut file)| {
            if multiple {
                file.path = format!("{id}/{}", file.path);
            }
            file
        })
        .collect();
    let summary = dump::put(&files, &args.output).await?;
    eprintln!(
        "dump: {} file(s), {} bytes -> {}",
        summary.file_count,
        summary.byte_count,
        args.output.display()
    );
    Ok(())
}

/// A file's table id plus the file, in document order.
type Entry = (String, DumpFile);

async fn resolve(args: &DumpArgs) -> Result<Vec<Entry>, CliError> {
    match args.input.as_deref() {
        Some("-") => from_document("-").await,
        Some(value) if document::is_document(value).await => from_document(value).await,
        Some(value) => from_table(value).await,
        None if std::io::stdin().is_terminal() => {
            Err("dump needs a table URI or a table/lake document".into())
        }
        None => from_document("-").await,
    }
}

async fn from_document(input: &str) -> Result<Vec<Entry>, CliError> {
    let mut entries = Vec::new();
    let mut envs: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    document::visit_input(input, async |record| {
        match record {
            Record::Begin(begin) => {
                envs.insert(begin.id.clone(), begin.env);
            }
            Record::File { id, file } => entries.push((
                id.clone(),
                DumpFile {
                    path: file.path,
                    uri: file.uri,
                    env: envs.get(&id).cloned().unwrap_or_default(),
                },
            )),
            Record::Table(info) => {
                let id = info.uri.clone();
                push_table(&mut entries, &info, id);
            }
            Record::Lake(lake) => {
                for table in lake.tables {
                    let info = table.info.ok_or_else(|| {
                        format!(
                            "table {} has no log; pipe the lake through `pqbench table` first",
                            table.name
                        )
                    })?;
                    push_table(&mut entries, &info, table.name);
                }
            }
            Record::TableRef(_) => {
                return Err("a table-ref names a table; pass it to `pqbench table` first".into());
            }
            Record::RemoteSource(_) => {
                return Err(
                    "a remote-source names tables; pass it to `pqbench table` first".into(),
                );
            }
            Record::LakeSource(_) => {
                return Err("a lake source lists tables; pass it to `pqbench lake` first".into());
            }
            Record::Commit { .. } | Record::End { .. } | Record::LakeBegin | Record::LakeEnd => {}
            Record::BytemassBegin | Record::BytemassRow { .. } | Record::BytemassEnd => {
                return Err("a bytemass stream goes to `pqbench viz`".into());
            }
        }
        Ok(())
    })
    .await?;
    if entries.is_empty() {
        return Err("document has no files to dump".into());
    }
    Ok(entries)
}

async fn from_table(uri: &str) -> Result<Vec<Entry>, CliError> {
    let info = table::load(&LoadRequest::new(uri, None, BTreeMap::new())).await?;
    let mut entries = Vec::new();
    let id = info.uri.clone();
    push_table(&mut entries, &info, id);
    Ok(entries)
}

fn push_table(entries: &mut Vec<Entry>, info: &TableInfo, id: String) {
    for file in &info.files {
        entries.push((
            id.clone(),
            DumpFile {
                path: file.path.clone(),
                uri: file.uri.clone(),
                env: info.env.clone(),
            },
        ));
    }
}
