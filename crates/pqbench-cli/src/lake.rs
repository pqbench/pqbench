use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use clap::Args;
use pqbench::lake::{self, Lake, LakeTable};
use serde::Serialize;

use crate::document::{self, Record};
use crate::emit::Emit;
use crate::filter::NameFilter;
use crate::CliError;

/// Arguments for `lake`.
#[derive(Args)]
pub(crate) struct LakeArgs {
    /// lake directory, object URI, a `pqbench.lake` document, or `-` for stdin
    input: Option<String>,
    /// zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// catalogs, schemas, prefixes, or tables to list at once
    #[arg(long, default_value = "4", value_name = "N")]
    concurrency: NonZeroUsize,
    /// path components below the walk root to search
    #[arg(long = "max-depth", default_value = "8", value_name = "N")]
    max_depth: NonZeroUsize,
    /// keep FQNs that match a glob or prefix (`main`, `main.default`, `main.default.events`)
    #[arg(long = "include", value_name = "PATTERN")]
    include: Vec<String>,
    /// drop FQNs that match a glob or prefix
    #[arg(long = "exclude", value_name = "PATTERN")]
    exclude: Vec<String>,
}

pub(crate) fn run(args: &LakeArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_async(args))
}

async fn run_async(args: &LakeArgs) -> Result<(), CliError> {
    let filter = NameFilter::new(args.include.clone(), args.exclude.clone());
    let mut emit = Emit::open("lake", args.output.as_deref())?;
    emit.write(&BeginRecord {
        kind: "pqbench.lake",
        version: 1,
        event: "begin",
    })?;
    let concurrency = args.concurrency.get();
    let max_depth = args.max_depth.get();
    let tables = match &args.input {
        None if !std::io::stdin().is_terminal() => {
            stream_document("-", &filter, concurrency, &mut emit).await?
        }
        None => {
            return Err("lake needs a directory, a URI, or a document on standard input".into())
        }
        Some(value) if document::looks_like_document(value) => {
            stream_document(value, &filter, concurrency, &mut emit).await?
        }
        Some(uri) if uri.contains("://") => {
            write_discovered_uri(uri, &filter, max_depth, &mut emit).await?
        }
        Some(path) => write_discovered(Path::new(path), &filter, max_depth, &mut emit)?,
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
    concurrency: usize,
    emit: &mut Emit,
) -> Result<usize, CliError> {
    let mut lake = None;
    let mut source = None;
    let reader: Box<dyn std::io::Read> = if input == "-" {
        Box::new(std::io::stdin())
    } else {
        document::open_file(Path::new(input))?
    };
    document::visit_records(reader, |record| {
        match record {
        Record::Lake(listed) => {
            lake = Some(listed);
            Ok(())
        }
        Record::LakeSource(listed) => {
            source = Some(listed);
            Ok(())
        }
        Record::LakeBegin | Record::LakeEnd => Ok(()),
        Record::TableRef(table) => write_ref(
            emit,
            &LakeTable {
                name: table.id,
                uri: table.uri,
                env: table.env,
                info: None,
            },
        )
        .map(|_| ()),
        Record::Table(_)
        | Record::RemoteSource(_)
        | Record::Begin(_)
        | Record::Log { .. }
        | Record::File { .. }
        | Record::End { .. }
        | Record::BytemassBegin
        | Record::BytemassFile(_)
        | Record::BytemassRow { .. }
        | Record::BytemassEnd => Err(
            "pqbench lake reads a directory, a URI, a pqbench.lake document, or a pqbench.lake-source"
                .into(),
        ),
    }
    })?;
    if let Some(source) = source {
        let endpoint = source.catalog_endpoint()?;
        let token = source.catalog_token();
        return match crate::catalog::protocol(&endpoint, token.as_deref())? {
            crate::catalog::Protocol::IcebergRest => {
                crate::iceberg::list_tables(&source, filter, concurrency, |table| {
                    write_ref(emit, &table).map(|_| ())
                })
                .await
            }
            crate::catalog::Protocol::Unity => {
                crate::unity::list_tables(&source, filter, concurrency, |table| {
                    write_ref(emit, &table).map(|_| ())
                })
                .await
            }
        };
    }
    if let Some(lake) = lake {
        return write_lake(&lake, filter, emit);
    }
    Err("empty lake document".into())
}

fn write_discovered(
    root: &Path,
    filter: &NameFilter,
    max_depth: usize,
    emit: &mut Emit,
) -> Result<usize, CliError> {
    write_lake(&lake::discover_at(root, Some(max_depth))?, filter, emit)
}

async fn write_discovered_uri(
    uri: &str,
    filter: &NameFilter,
    max_depth: usize,
    emit: &mut Emit,
) -> Result<usize, CliError> {
    write_lake(
        &lake::discover_uri_at(uri, &BTreeMap::new(), Some(max_depth)).await?,
        filter,
        emit,
    )
}

fn write_lake(lake: &Lake, filter: &NameFilter, emit: &mut Emit) -> Result<usize, CliError> {
    let mut tables = 0usize;
    for table in &lake.tables {
        if filter.keeps(&table.name) {
            write_ref(emit, table)?;
            tables += 1;
        }
    }
    if tables == 0 {
        return Err("lake listed no tables after include/exclude".into());
    }
    Ok(tables)
}

fn write_ref(emit: &mut Emit, table: &LakeTable) -> Result<(), CliError> {
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
    #[serde(skip_serializing_if = "map_empty")]
    env: &'a std::collections::BTreeMap<String, String>,
}

fn map_empty(env: &&std::collections::BTreeMap<String, String>) -> bool {
    env.is_empty()
}
