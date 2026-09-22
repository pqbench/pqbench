use std::collections::BTreeMap;
use std::io::IsTerminal;

use clap::Args;
use pqbench::lake::Lake;
use pqbench::table::{self, LoadRequest, TableInfo};

use crate::document::{self, Document};
use crate::CliError;

/// Arguments for `table`.
#[derive(Args)]
pub(crate) struct TableArgs {
    /// table URI, a document file, or `-` for standard input
    input: Option<String>,
    /// snapshot version; defaults to the latest version
    #[arg(long)]
    version: Option<u64>,
}

pub(crate) fn run(args: &TableArgs) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match runtime.block_on(load(args))? {
        Loaded::Table(info) => document::write_table(&info),
        Loaded::Lake(lake) => crate::lake::write(&lake),
    }
}

enum Loaded {
    Table(TableInfo),
    Lake(Lake),
}

async fn load(args: &TableArgs) -> Result<Loaded, CliError> {
    match input(args)? {
        TableInput::Uri(uri) => Ok(Loaded::Table(
            table::load(&LoadRequest {
                uri,
                version: args.version,
                env: BTreeMap::new(),
            })
            .await?,
        )),
        TableInput::RemoteSource { uri, env } => {
            document::apply_env(&env)?;
            Ok(Loaded::Table(
                table::load(&LoadRequest {
                    uri,
                    version: args.version,
                    env,
                })
                .await?,
            ))
        }
        TableInput::Table(info) => Ok(Loaded::Table(info)),
        TableInput::Lake(lake) => Ok(Loaded::Lake(resolve_lake(lake, args.version).await?)),
    }
}

async fn resolve_lake(mut lake: Lake, version: Option<u64>) -> Result<Lake, CliError> {
    for table in &mut lake.tables {
        if table.info.is_some() {
            continue;
        }
        document::apply_env(&table.env)?;
        table.info = Some(
            table::load(&LoadRequest {
                uri: table.uri.clone(),
                version,
                env: table.env.clone(),
            })
            .await?,
        );
    }
    Ok(lake)
}

enum TableInput {
    Uri(String),
    RemoteSource {
        uri: String,
        env: BTreeMap<String, String>,
    },
    Table(TableInfo),
    Lake(Lake),
}

fn input(args: &TableArgs) -> Result<TableInput, CliError> {
    match &args.input {
        None if !std::io::stdin().is_terminal() => from_document("-"),
        None => Err("table needs a URI or a document on standard input".into()),
        Some(value) if document::looks_like_json(value) => from_document(value),
        Some(uri) => Ok(TableInput::Uri(uri.clone())),
    }
}

fn from_document(input: &str) -> Result<TableInput, CliError> {
    match document::read_document(input)? {
        Document::RemoteSource(source) => {
            if source.inputs.len() != 1 {
                return Err(format!(
                    "a table document names one table, but the source names {} inputs",
                    source.inputs.len()
                )
                .into());
            }
            let mut inputs = source.inputs;
            Ok(TableInput::RemoteSource {
                uri: inputs.remove(0),
                env: source.env,
            })
        }
        Document::Table(info) => Ok(TableInput::Table(info)),
        Document::Lake(lake) => Ok(TableInput::Lake(lake)),
        Document::LakeSource(_) => {
            Err("a lake source lists tables; pass it to `pqbench lake` first".into())
        }
    }
}
