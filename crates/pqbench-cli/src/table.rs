use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::{self, StreamExt};
use pqbench::table::{self, info, FileSelection, LoadEvent, LoadRequest, TableInfo};
use serde::Serialize;

use crate::document::{self, Record, TableRef};
use crate::emit::{Align, Emitter, Format, Row};
use crate::file_selection::{self, FileSelectionArgs};
use crate::source::{self, read_storage_input, ref_env, table_ref};
use crate::CliError;

/// Arguments for `table`: load one table, or resolve refs with `info`.
#[derive(Args)]
#[command(args_conflicts_with_subcommands = true)]
pub(crate) struct TableArgs {
    #[command(subcommand)]
    command: Option<TableCommand>,
    #[command(flatten)]
    load: LoadArgs,
}

#[derive(Subcommand)]
pub(crate) enum TableCommand {
    /// Fill each table-ref's storage path from its record
    Info(InfoArgs),
    /// List the table's natural partitions, grouped by commit time
    Ls(LsArgs),
}

/// Arguments for loading one table.
#[derive(Args)]
pub(crate) struct LoadArgs {
    #[command(flatten)]
    selection: FileSelectionArgs,
    /// table URI, a document file, or `-` for standard input
    input: Option<String>,
    /// snapshot version; defaults to the latest version
    #[arg(long)]
    version: Option<u64>,
    /// omit min/max/null maps (keep num_records and bytes_per_row)
    #[arg(long)]
    no_stats: bool,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

/// Arguments for `table info`.
#[derive(Args)]
pub(crate) struct InfoArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

pub(crate) async fn run(args: &TableArgs) -> Result<(), CliError> {
    match &args.command {
        Some(TableCommand::Info(info)) => run_info(info).await,
        Some(TableCommand::Ls(ls)) => run_ls(ls).await,
        None => run_load(&args.load).await,
    }
}

async fn run_load(args: &LoadArgs) -> Result<(), CliError> {
    let selection = args.selection.parse()?;
    match &args.input {
        None if !std::io::stdin().is_terminal() => stream("-", args, &selection).await,
        None => Err("table needs a URI or a document on standard input".into()),
        Some(value) => {
            // An Iceberg `.metadata.json` is JSON on disk but names a table,
            // not a pqbench document; keep it on the table path.
            let metadata_json = value.ends_with(".metadata.json");
            if !metadata_json && document::is_document(value).await {
                stream(value, args, &selection).await
            } else {
                load_path(value, args, &selection).await
            }
        }
    }
}

/// Fill each `pqbench.table-ref`'s storage path from its record address.
async fn run_info(args: &InfoArgs) -> Result<(), CliError> {
    if std::io::stdin().is_terminal() {
        return Err("table info reads pqbench.table-ref records on standard input".into());
    }
    let mut refs = Vec::new();
    document::visit_input("-", async |record| {
        match record {
            Record::TableRef(table_ref) => refs.push(table_ref),
            Record::Lake(lake) => {
                for table in lake.tables {
                    refs.push(TableRef {
                        id: table.name,
                        uri: table.uri.clone(),
                        storage_path: Some(table.uri),
                        env: table.env,
                    });
                }
            }
            Record::LakeBegin | Record::LakeEnd => {}
            _ => return Err("table info reads pqbench.table-ref records on standard input".into()),
        }
        Ok(())
    })
    .await?;
    let token = std::env::var("PQB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let mut tables = 0usize;
    let mut resolved = stream::iter(refs)
        .map(|table_ref| async {
            let mut table_ref = table_ref;
            if table_ref.storage_path.is_none() {
                table_ref.storage_path = Some(info::read(&table_ref.uri, token.as_deref()).await?);
            }
            Ok::<_, CliError>(table_ref)
        })
        .buffered(args.fan_out);
    while let Some(table_ref) = resolved.next().await {
        let table_ref = table_ref?;
        emit.write_row(&TableRefRecord {
            kind: "pqbench.table-ref",
            version: 1,
            id: &table_ref.id,
            uri: &table_ref.uri,
            storage_path: table_ref.storage_path.as_deref(),
            env: &table_ref.env,
        })
        .await?;
        tables += 1;
    }
    emit.finish(&format!("tables: {tables}\n")).await
}

async fn load_path(uri: &str, args: &LoadArgs, selection: &FileSelection) -> Result<(), CliError> {
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if !selection.unrestricted() {
        let info = load_info(uri.to_owned(), BTreeMap::new(), args, selection).await?;
        document::write_table_records(&mut emit, uri, &info).await?;
        return emit
            .finish(&summary(
                1,
                info.files.len(),
                file_bytes(&info),
                args.output.as_deref(),
            ))
            .await;
    }
    let request = load_request(uri.to_string(), BTreeMap::new(), args)?;
    let (files, bytes) = emit_table(&request, uri, &mut emit).await?;
    emit.finish(&summary(1, files, bytes, args.output.as_deref()))
        .await
}

/// Load one table and apply `selection`; buffers the file list.
async fn load_info(
    uri: String,
    env: BTreeMap<String, String>,
    args: &LoadArgs,
    selection: &FileSelection,
) -> Result<TableInfo, CliError> {
    let mut info = table::load(&load_request(uri, env, args)?).await?;
    file_selection::apply(selection, &mut info)?;
    Ok(info)
}

async fn emit_table(
    request: &LoadRequest,
    id: &str,
    emit: &mut Emitter,
) -> Result<(usize, u64), CliError> {
    let request = request
        .clone()
        .with_collect_files(false)
        .with_collect_log(false);
    let mut files = 0usize;
    let mut bytes = 0u64;
    let info = table::visit_load(&request, async |event| {
        let result = match event {
            LoadEvent::BEGIN { info } => document::write_table_begin(emit, id, info).await,
            LoadEvent::COMMIT { commit } => document::write_table_commit(emit, id, commit).await,
            LoadEvent::FILE { file } => {
                files += 1;
                bytes += file.size_bytes;
                document::write_table_file(emit, id, file).await
            }
        };
        result.map_err(|error| table::Error::new(error.to_string()))
    })
    .await?;
    document::write_table_end(emit, id, &info.partitions).await?;
    Ok((files, bytes))
}

/// Load one table, selecting when the policy is restricted, else stream it.
async fn load_one(
    uri: String,
    env: BTreeMap<String, String>,
    id: &str,
    args: &LoadArgs,
    selection: &FileSelection,
    emit: &mut Emitter,
) -> Result<(usize, u64), CliError> {
    if selection.unrestricted() {
        let request = load_request(uri, env, args)?;
        emit_table(&request, id, emit).await
    } else {
        let info = load_info(uri, env, args, selection).await?;
        let files = info.files.len();
        let bytes = file_bytes(&info);
        document::write_table_records(emit, id, &info).await?;
        Ok((files, bytes))
    }
}

fn load_request(
    uri: String,
    env: BTreeMap<String, String>,
    args: &LoadArgs,
) -> Result<LoadRequest, CliError> {
    Ok(LoadRequest::new(uri, args.version, env).with_file_stats(!args.no_stats))
}

async fn stream(input: &str, args: &LoadArgs, selection: &FileSelection) -> Result<(), CliError> {
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let mut tables = 0usize;
    let mut files = 0usize;
    let mut bytes = 0u64;
    document::visit_input(input, async |record| {
        match record {
            Record::TableRef(table_ref) => {
                let storage_path = table_ref.storage_path.clone().ok_or_else(|| {
                    CliError::from(format!(
                        "table-ref {} has no storage path; run `pqbench table info` first",
                        table_ref.id
                    ))
                })?;
                let (file_count, size) = load_one(
                    storage_path,
                    table_ref.env,
                    &table_ref.id,
                    args,
                    selection,
                    &mut emit,
                )
                .await?;
                tables += 1;
                files += file_count;
                bytes += size;
            }
            Record::RemoteSource(source) => {
                for uri in source.inputs {
                    let (file_count, size) = load_one(
                        uri.clone(),
                        source.env.clone(),
                        &uri,
                        args,
                        selection,
                        &mut emit,
                    )
                    .await?;
                    tables += 1;
                    files += file_count;
                    bytes += size;
                }
            }
            Record::Table(mut info) => {
                file_selection::apply(selection, &mut info)?;
                add(&info, &mut tables, &mut files, &mut bytes);
                document::write_table_records(&mut emit, &info.uri, &info).await?;
            }
            Record::Lake(lake) => {
                for table in lake.tables {
                    let (file_count, size) = load_one(
                        table.uri,
                        table.env,
                        &table.name,
                        args,
                        selection,
                        &mut emit,
                    )
                    .await?;
                    tables += 1;
                    files += file_count;
                    bytes += size;
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
            | Record::BytemassPage
            | Record::BytemassEnd => {
                return Err("a bytemass stream goes to `pqbench viz`".into());
            }
        }
        Ok(())
    })
    .await?;
    emit.finish(&summary(tables, files, bytes, args.output.as_deref()))
        .await
}

/// The document `table info` writes, one line per resolved ref.
#[derive(Serialize)]
struct TableRefRecord<'a> {
    kind: &'static str,
    version: u32,
    id: &'a str,
    uri: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_path: Option<&'a str>,
    #[serde(skip_serializing_if = "is_empty_env")]
    env: &'a BTreeMap<String, String>,
}

fn is_empty_env(env: &&BTreeMap<String, String>) -> bool {
    env.is_empty()
}

impl Row for TableRefRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "uri", "storage path"];
    const ALIGN: &'static [Align] = &[Align::Left; 3];

    fn cells(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.uri.to_string(),
            self.storage_path.unwrap_or_default().to_string(),
        ]
    }
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

/// Arguments for `table ls`.
#[derive(Args)]
pub(crate) struct LsArgs {
    /// table URI or a document file; without it, read table refs on stdin
    input: Option<String>,
    /// window width, epoch-aligned: e.g. 1h, 1d, 1w
    #[arg(long, default_value = "1d")]
    every: String,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// tables in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
}

/// List a table's natural partitions: commits grouped into commit-time windows.
async fn run_ls(args: &LsArgs) -> Result<(), CliError> {
    let window = parse_window(&args.every)?;
    let context = read_storage_input("table ls").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let mut partitions = 0usize;
    match &args.input {
        Some(uri) if uri != "-" => {
            if context.first.is_some() {
                return Err(
                    "table ls takes a table URI or a pqbench.table-ref v2 stream, not both".into(),
                );
            }
            let env = context.source.env.clone();
            partitions += list_partitions(uri, &env, window, &mut emit).await?;
        }
        _ => {
            if !context.piped {
                return Err("table ls needs a table URI or a pqbench.table-ref v2 stream".into());
            }
            let source = context.source;
            let records = source::records("table ls", context.first, context.lines);
            let mut reads = records
                .map(|record| async {
                    let record = record?;
                    table_ref("table ls", &record)?;
                    let uri = record["storage_path"]
                        .as_str()
                        .filter(|path| !path.is_empty())
                        .ok_or_else(|| {
                            CliError::from(
                                "a table-ref has no storage path; run `pqbench table info` first",
                            )
                        })?
                        .to_string();
                    let env = ref_env(&record, &source.env);
                    let found = table::ls::list(&uri, &env, window)
                        .await
                        .map_err(|error| CliError::from(error.to_string()))?;
                    Ok::<_, CliError>((uri, found))
                })
                .buffer_unordered(args.fan_out.max(1));
            while let Some(result) = reads.next().await {
                let (uri, found) = result?;
                for partition in &found {
                    emit.write_row(&partition_record(&uri, partition)).await?;
                    partitions += 1;
                }
            }
        }
    }
    emit.finish(&format!("partitions: {partitions}\n")).await
}

/// Emit one table's partitions; returns how many were written.
async fn list_partitions(
    uri: &str,
    env: &BTreeMap<String, String>,
    window: i64,
    emit: &mut Emitter,
) -> Result<usize, CliError> {
    let found = table::ls::list(uri, env, window)
        .await
        .map_err(|error| CliError::from(error.to_string()))?;
    let mut count = 0;
    for partition in &found {
        emit.write_row(&partition_record(uri, partition)).await?;
        count += 1;
    }
    Ok(count)
}

/// Parse a window width: a positive integer and a unit (`m`, `h`, `d`, `w`).
fn parse_window(value: &str) -> Result<i64, CliError> {
    let (number, unit) = value.split_at(
        value
            .len()
            .checked_sub(1)
            .ok_or_else(|| CliError::from("--every is empty"))?,
    );
    let count: i64 = number
        .parse()
        .map_err(|_| CliError::from(format!("--every `{value}` needs a number and a unit")))?;
    if count <= 0 {
        return Err(format!("--every must be positive: {value}").into());
    }
    let millis = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return Err(format!("--every unit must be m, h, d, or w: {value}").into()),
    };
    Ok(count * millis)
}

/// The document `table ls` writes, one line per partition.
#[derive(Serialize)]
struct PartitionRecord<'a> {
    kind: &'static str,
    version: u32,
    table: &'a str,
    definition: Definition,
    commits: Vec<CommitCell>,
}

/// A partition's definition: a natural commit-time window.
#[derive(Serialize)]
struct Definition {
    kind: &'static str,
    first_time: i64,
    last_time: i64,
}

/// One commit in a partition, in the emitted document.
#[derive(Serialize)]
struct CommitCell {
    version: u64,
    commit_time: i64,
}

impl Row for PartitionRecord<'_> {
    const HEADER: &'static [&'static str] = &["table", "first_time", "last_time", "commits"];
    const ALIGN: &'static [Align] = &[Align::Left, Align::Right, Align::Right, Align::Right];

    fn cells(&self) -> Vec<String> {
        vec![
            self.table.to_string(),
            self.definition.first_time.to_string(),
            self.definition.last_time.to_string(),
            self.commits.len().to_string(),
        ]
    }
}

fn partition_record<'a>(table: &'a str, partition: &table::ls::Partition) -> PartitionRecord<'a> {
    PartitionRecord {
        kind: "pqbench.partition",
        version: 1,
        table,
        definition: Definition {
            kind: "natural",
            first_time: partition.first_time,
            last_time: partition.last_time,
        },
        commits: partition
            .commits
            .iter()
            .map(|commit| CommitCell {
                version: commit.version,
                commit_time: commit.commit_time,
            })
            .collect(),
    }
}
