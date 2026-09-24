use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use clap::Args;
use pqbench::lake::filter::NameFilter;
use pqbench::lake::{self, Lake, LakeTable};
use pqbench::unity;
use serde::Serialize;

use crate::document::{self, Record};
use crate::emit::Emitter;
use crate::CliError;

/// Arguments for `lake`.
#[derive(Args)]
pub(crate) struct LakeArgs {
    /// lake directory, a `pqbench.lake` document, or `-` for standard input
    input: Option<String>,
    /// zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// keep FQNs that match a glob or prefix (`main`, `main.default`, `main.default.events`)
    #[arg(long = "include", value_name = "PATTERN")]
    include: Vec<String>,
    /// drop FQNs that match a glob or prefix
    #[arg(long = "exclude", value_name = "PATTERN")]
    exclude: Vec<String>,
}

/// List a lake as `pqbench.table-ref` records, one table per later process.
pub(crate) async fn run(args: &LakeArgs) -> Result<(), CliError> {
    let filter = NameFilter::new(args.include.clone(), args.exclude.clone());
    let mut emit = Emitter::open("lake", args.output.as_deref())?;
    emit.write(&BeginRecord {
        kind: "pqbench.lake",
        version: 1,
        event: "begin",
    })?;
    let tables = match &args.input {
        None if !std::io::stdin().is_terminal() => stream_document("-", &filter, &mut emit).await?,
        None => return Err("lake needs a directory or a document on standard input".into()),
        Some(value) => {
            if document::is_document(value).await {
                stream_document(value, &filter, &mut emit).await?
            } else {
                write_discovered(Path::new(value), &filter, &mut emit)?
            }
        }
    };
    emit.write(&EndRecord {
        kind: "pqbench.lake",
        event: "end",
        table_count: tables,
    })?;
    emit.finish(&format!(
        "tables: {tables}{}\n",
        args.output
            .as_ref()
            .map(|path| format!("\noutput: {}", path.display()))
            .unwrap_or_default()
    ))
}

async fn stream_document(
    input: &str,
    filter: &NameFilter,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let mut tables = 0usize;
    let mut source = None;
    let mut lake = None;
    document::visit_input(input, async |record| {
        match record {
            Record::TableRef(table) => {
                write_ref(
                    emit,
                    &LakeTable {
                        name: table.id,
                        uri: table.uri,
                        env: table.env,
                        info: None,
                    },
                )?;
                tables += 1;
            }
            Record::Lake(listed) => lake = Some(listed),
            Record::LakeSource(listed) => source = Some(listed),
            Record::LakeBegin | Record::LakeEnd => {}
            Record::Table(_)
            | Record::RemoteSource(_)
            | Record::Begin(_)
            | Record::Commit { .. }
            | Record::File { .. }
            | Record::End { .. } => return Err(
                "pqbench lake reads a directory, a pqbench.lake document, or a pqbench.lake-source"
                    .into(),
            ),
        }
        Ok(())
    })
    .await?;
    if let Some(source) = source {
        for table in unity::list_tables(&source, filter)? {
            write_ref(emit, &table)?;
            tables += 1;
        }
    }
    if let Some(lake) = lake {
        tables += write_lake(&lake, filter, emit)?;
    }
    if tables == 0 {
        return Err("lake listed no tables after include/exclude".into());
    }
    Ok(tables)
}

fn write_discovered(
    root: &Path,
    filter: &NameFilter,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    write_lake(&lake::discover(root)?, filter, emit)
}

fn write_lake(lake: &Lake, filter: &NameFilter, emit: &mut Emitter) -> Result<usize, CliError> {
    let mut tables = 0usize;
    for table in &lake.tables {
        if filter.keeps(&table.name) {
            write_ref(emit, table)?;
            tables += 1;
        }
    }
    Ok(tables)
}

fn write_ref(emit: &mut Emitter, table: &LakeTable) -> Result<(), CliError> {
    emit.write(&TableRefRecord {
        kind: "pqbench.table-ref",
        version: 1,
        id: &table.name,
        uri: &table.uri,
        env: &table.env,
    })
}

#[derive(Serialize)]
struct BeginRecord {
    kind: &'static str,
    version: u32,
    event: &'static str,
}

#[derive(Serialize)]
struct EndRecord {
    kind: &'static str,
    event: &'static str,
    table_count: usize,
}

#[derive(Serialize)]
struct TableRefRecord<'a> {
    kind: &'static str,
    version: u32,
    id: &'a str,
    uri: &'a str,
    #[serde(skip_serializing_if = "is_empty_env")]
    env: &'a BTreeMap<String, String>,
}

fn is_empty_env(env: &&BTreeMap<String, String>) -> bool {
    env.is_empty()
}
