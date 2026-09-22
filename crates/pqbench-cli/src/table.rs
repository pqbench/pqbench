use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::Args;
use pqbench::pattern::Selection;
use pqbench::table::{self, LoadRequest, TableInfo};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::document::{self, Record, TableRef};
use crate::emit::Emit;
use crate::CliError;

/// Arguments for `table`.
#[derive(Args)]
pub(crate) struct TableArgs {
    /// table URI, lake/table/remote-source document, or `-`
    input: Option<String>,
    /// Delta commit or Iceberg snapshot id (default: latest)
    #[arg(long)]
    version: Option<u64>,
    /// drop Delta files with log mtime before this RFC3339 UTC instant
    #[arg(long, value_name = "TIME")]
    exclude_modified_before: Option<String>,
    /// drop Delta files with log mtime after this RFC3339 UTC instant
    #[arg(long, value_name = "TIME")]
    exclude_modified_after: Option<String>,
    /// drop Delta files whose add version is below N
    #[arg(long, value_name = "N")]
    exclude_version_before: Option<u64>,
    /// drop Delta files whose add version is above N
    #[arg(long, value_name = "N")]
    exclude_version_after: Option<u64>,
    /// drop snapshots created before this RFC3339 UTC instant
    #[arg(long, value_name = "TIME")]
    exclude_snapshot_before: Option<String>,
    /// drop snapshots created after this RFC3339 UTC instant
    #[arg(long, value_name = "TIME")]
    exclude_snapshot_after: Option<String>,
    /// zstd NDJSON stream (required on a terminal)
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// tables to load at once
    #[arg(long, default_value = "4", value_name = "N")]
    concurrency: NonZeroUsize,
}

pub(crate) fn run(args: &TableArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_async(args))
}

async fn run_async(args: &TableArgs) -> Result<(), CliError> {
    match &args.input {
        None if !std::io::stdin().is_terminal() => stream("-", args).await,
        None => Err("table needs a URI or a document on standard input".into()),
        Some(value) if document::looks_like_document(value) => stream(value, args).await,
        Some(uri) => load_one(uri.clone(), BTreeMap::new(), args).await,
    }
}

async fn load_one(
    uri: String,
    env: BTreeMap<String, String>,
    args: &TableArgs,
) -> Result<(), CliError> {
    let info = table::load(
        &LoadRequest::new(uri.clone(), args.version, env).with_selection(selection(args)?),
    )
    .await?;
    let mut emit = Emit::open("table", args.output.as_deref())?;
    document::write_table_records(&mut emit, &uri, &info)?;
    emit.finish(&summary(
        1,
        info.files.len(),
        file_bytes(&info),
        args.output.as_deref(),
    ))
}

async fn stream(input: &str, args: &TableArgs) -> Result<(), CliError> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Result<Record, String>>();
    let path = input.to_string();
    std::thread::spawn(move || {
        let reader: Box<dyn std::io::Read> = if path == "-" {
            Box::new(std::io::stdin())
        } else {
            match document::open_file(std::path::Path::new(&path)) {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = tx.send(Err(error.to_string()));
                    return;
                }
            }
        };
        if let Err(error) = document::visit_records(reader, |record| {
            tx.send(Ok(record)).map_err(|_| "table input closed".into())
        }) {
            let _ = tx.send(Err(error.to_string()));
        }
    });

    let mut emit = Emit::open("table", args.output.as_deref())?;
    let mut set: JoinSet<Result<(String, TableInfo), String>> = JoinSet::new();
    let mut tables = 0usize;
    let mut files = 0usize;
    let mut bytes = 0u64;
    let concurrency = args.concurrency.get();
    let version = args.version;
    let selected = selection(args)?;

    loop {
        tokio::select! {
            done = set.join_next(), if !set.is_empty() => {
                if let Some(done) = done {
                    emit_loaded(&mut emit, done, &mut tables, &mut files, &mut bytes)?;
                }
            }
            rec = rx.recv() => {
                match rec {
                    None => break,
                    Some(Err(error)) => return Err(error.into()),
                    Some(Ok(record)) => {
                        queue_record(
                            record,
                            &mut set,
                            &mut emit,
                            &mut tables,
                            &mut files,
                            &mut bytes,
                            concurrency,
                            version,
                            selected.clone(),
                        )
                        .await?;
                    }
                }
            }
        }
    }
    while let Some(done) = set.join_next().await {
        emit_loaded(&mut emit, done, &mut tables, &mut files, &mut bytes)?;
    }
    emit.finish(&summary(tables, files, bytes, args.output.as_deref()))
}

#[allow(clippy::too_many_arguments)]
async fn queue_record(
    record: Record,
    set: &mut JoinSet<Result<(String, TableInfo), String>>,
    emit: &mut Emit,
    tables: &mut usize,
    files: &mut usize,
    bytes: &mut u64,
    concurrency: usize,
    version: Option<u64>,
    selected: Selection,
) -> Result<(), CliError> {
    match record {
        Record::TableRef(table_ref) => {
            spawn_ref(
                set,
                emit,
                tables,
                files,
                bytes,
                concurrency,
                version,
                selected,
                table_ref,
            )
            .await
        }
        Record::RemoteSource(source) => {
            for uri in source.inputs {
                spawn_ref(
                    set,
                    emit,
                    tables,
                    files,
                    bytes,
                    concurrency,
                    version,
                    selected.clone(),
                    TableRef {
                        id: uri.clone(),
                        uri,
                        env: source.env.clone(),
                    },
                )
                .await?;
            }
            Ok(())
        }
        Record::Table(mut info) => {
            if !selected.is_default() {
                info.selection = selected;
                table::apply_file_selection(&mut info)?;
            }
            *tables += 1;
            *files += info.files.len();
            *bytes += file_bytes(&info);
            document::write_table_records(emit, &info.uri, &info)
        }
        Record::Lake(lake) => {
            for table in lake.tables {
                spawn_ref(
                    set,
                    emit,
                    tables,
                    files,
                    bytes,
                    concurrency,
                    version,
                    selected.clone(),
                    TableRef {
                        id: table.name,
                        uri: table.uri,
                        env: table.env,
                    },
                )
                .await?;
            }
            Ok(())
        }
        Record::LakeSource(_) => {
            Err("a lake source lists tables; pass it to `pqbench lake` first".into())
        }
        Record::LakeBegin | Record::LakeEnd => Ok(()),
        Record::Begin(_) | Record::Log { .. } | Record::File { .. } | Record::End { .. } => {
            Err("a loaded table stream goes to `pqbench bytemass`, not `pqbench table`".into())
        }
        Record::BytemassBegin | Record::BytemassRow { .. } | Record::BytemassEnd => {
            Err("a bytemass stream goes to `pqbench viz`".into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_ref(
    set: &mut JoinSet<Result<(String, TableInfo), String>>,
    emit: &mut Emit,
    tables: &mut usize,
    files: &mut usize,
    bytes: &mut u64,
    concurrency: usize,
    version: Option<u64>,
    selected: Selection,
    table_ref: TableRef,
) -> Result<(), CliError> {
    while set.len() >= concurrency {
        if let Some(done) = set.join_next().await {
            emit_loaded(emit, done, tables, files, bytes)?;
        }
    }
    set.spawn(async move {
        let info = table::load(
            &LoadRequest::new(table_ref.uri, version, table_ref.env).with_selection(selected),
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok((table_ref.id, info))
    });
    Ok(())
}

fn emit_loaded(
    emit: &mut Emit,
    done: Result<Result<(String, TableInfo), String>, tokio::task::JoinError>,
    tables: &mut usize,
    files: &mut usize,
    bytes: &mut u64,
) -> Result<(), CliError> {
    let (id, info) = done.map_err(|error| error.to_string())??;
    *tables += 1;
    *files += info.files.len();
    *bytes += file_bytes(&info);
    document::write_table_records(emit, &id, &info)
}

fn file_bytes(info: &TableInfo) -> u64 {
    info.bytes()
}

fn selection(args: &TableArgs) -> Result<Selection, CliError> {
    let selected = Selection {
        exclude_modified_before: args.exclude_modified_before.clone(),
        exclude_modified_after: args.exclude_modified_after.clone(),
        exclude_version_before: args.exclude_version_before,
        exclude_version_after: args.exclude_version_after,
        exclude_snapshot_before: args.exclude_snapshot_before.clone(),
        exclude_snapshot_after: args.exclude_snapshot_after.clone(),
        ..Selection::default()
    };
    for time in [
        &selected.exclude_modified_before,
        &selected.exclude_modified_after,
        &selected.exclude_snapshot_before,
        &selected.exclude_snapshot_after,
    ]
    .into_iter()
    .flatten()
    {
        table::validate_time(time)?;
    }
    Ok(selected)
}

fn summary(tables: usize, files: usize, bytes: u64, output: Option<&std::path::Path>) -> String {
    let mut out = format!("tables: {tables}\nfiles: {files} ({bytes} bytes)\n");
    if let Some(path) = output {
        out.push_str(&format!("output: {}\n", path.display()));
    }
    out
}
