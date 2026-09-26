use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Args;
use pqbench::filter::Filter;
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
    /// latest snapshot created at or before this instant
    #[arg(long = "snapshot-at", value_name = "TIME")]
    snapshot_time: Option<String>,
    /// keep files matching an AIP-160 expression, e.g.
    /// `update_time >= "2024-01-01" AND size_bytes > 0`
    #[arg(long, value_name = "EXPR")]
    filter: Option<String>,
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
    let info = load_info(
        uri.to_string(),
        BTreeMap::new(),
        args.version,
        args.snapshot_time.clone(),
        filter(args)?,
    )
    .await?;
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
    snapshot_time: Option<String>,
    filter: Option<Filter>,
) -> Result<TableInfo, CliError> {
    let mut request = LoadRequest::new(uri, version, env);
    if let Some(instant) = snapshot_time {
        request = request.set_snapshot_time(instant);
    }
    if let Some(filter) = filter {
        request = request.set_filter(filter);
    }
    Ok(table::load(&request).await?)
}

async fn stream(input: &str, args: &TableArgs) -> Result<(), CliError> {
    let mut emit = Emitter::open("table", args.output.as_deref())?;
    let mut tables = 0usize;
    let mut files = 0usize;
    let mut bytes = 0u64;
    let snapshot_time = args.snapshot_time.clone();
    let filter = filter(args)?;
    document::visit_input(input, async |record| {
        match record {
            Record::TableRef(table_ref) => {
                let info = load_info(
                    table_ref.uri,
                    table_ref.env,
                    args.version,
                    snapshot_time.clone(),
                    filter.clone(),
                )
                .await?;
                add(&info, &mut tables, &mut files, &mut bytes);
                document::write_table_records(&mut emit, &table_ref.id, &info)?;
            }
            Record::RemoteSource(source) => {
                for uri in source.inputs {
                    let info = load_info(
                        uri.clone(),
                        source.env.clone(),
                        args.version,
                        snapshot_time.clone(),
                        filter.clone(),
                    )
                    .await?;
                    add(&info, &mut tables, &mut files, &mut bytes);
                    document::write_table_records(&mut emit, &uri, &info)?;
                }
            }
            Record::Table(info) => {
                if filter.is_some() {
                    return Err(
                        "--filter loads a table from a URI; a pqbench.table document is already resolved"
                            .into(),
                    );
                }
                add(&info, &mut tables, &mut files, &mut bytes);
                document::write_table_records(&mut emit, &info.uri, &info)?;
            }
            Record::Lake(lake) => {
                for table in lake.tables {
                    let info = load_info(
                        table.uri,
                        table.env,
                        args.version,
                        snapshot_time.clone(),
                        filter.clone(),
                    )
                    .await?;
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
            Record::BytemassBegin | Record::BytemassRow { .. } | Record::BytemassEnd => {
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
    info.bytes()
}

fn filter(args: &TableArgs) -> Result<Option<Filter>, CliError> {
    Ok(args.filter.as_deref().map(Filter::parse).transpose()?)
}

fn summary(tables: usize, files: usize, bytes: u64, output: Option<&std::path::Path>) -> String {
    let mut out = format!("tables: {tables}\nfiles: {files} ({bytes} bytes)\n");
    if let Some(path) = output {
        out.push_str(&format!("output: {}\n", path.display()));
    }
    out
}
