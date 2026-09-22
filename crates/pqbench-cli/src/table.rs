use std::collections::BTreeMap;
use std::io::IsTerminal;

use clap::Args;
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
    let info = runtime.block_on(load(args))?;
    document::write_table(&info)
}

async fn load(args: &TableArgs) -> Result<TableInfo, CliError> {
    match input(args)? {
        TableInput::Uri(uri) => Ok(table::load(&LoadRequest {
            uri,
            version: args.version,
            env: BTreeMap::new(),
        })
        .await?),
        TableInput::RemoteSource { uri, env } => {
            document::apply_env(&env)?;
            Ok(table::load(&LoadRequest {
                uri,
                version: args.version,
                env,
            })
            .await?)
        }
        TableInput::Table(info) => Ok(info),
    }
}

enum TableInput {
    Uri(String),
    RemoteSource {
        uri: String,
        env: BTreeMap<String, String>,
    },
    Table(TableInfo),
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
    }
}
