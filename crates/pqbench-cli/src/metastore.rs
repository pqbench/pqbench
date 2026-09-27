use std::io::IsTerminal;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use pqbench::metastore::info;
use serde::{Deserialize, Serialize};

use crate::emit::{Align, Emitter, Format, Row};
use crate::CliError;

/// Arguments for `metastore`: the endpoint's metastore.
#[derive(Args)]
pub(crate) struct MetastoreArgs {
    #[command(subcommand)]
    command: MetastoreCommand,
}

#[derive(Subcommand)]
pub(crate) enum MetastoreCommand {
    /// Show the endpoint's metastore record
    Info(InfoArgs),
}

#[derive(Args)]
pub(crate) struct InfoArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the zstd NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

/// The document `metastore info` reads: a `pqbench.lake-source`.
#[derive(Deserialize)]
struct Source {
    version: u32,
    endpoint: String,
    #[serde(default)]
    token: Option<String>,
}

/// The document `metastore info` writes.
#[derive(Serialize)]
struct MetastoreRecord<'a> {
    kind: &'static str,
    version: u32,
    name: &'a str,
    id: &'a str,
    cloud: &'a str,
    region: &'a str,
}

impl Row for MetastoreRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "id", "cloud", "region"];
    const ALIGN: &'static [Align] = &[Align::Left; 4];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.to_string(),
            self.id.to_string(),
            self.cloud.to_string(),
            self.region.to_string(),
        ]
    }
}

pub(crate) async fn run(args: &MetastoreArgs) -> Result<(), CliError> {
    match &args.command {
        MetastoreCommand::Info(info) => run_info(info).await,
    }
}

async fn run_info(args: &InfoArgs) -> Result<(), CliError> {
    let source = read_source().await?;
    let metastore = info::read(&source.endpoint, source.token.as_deref()).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    emit.write_row(&MetastoreRecord {
        kind: "pqbench.metastore",
        version: 1,
        name: &metastore.name,
        id: &metastore.id,
        cloud: &metastore.cloud,
        region: &metastore.region,
    })?;
    emit.finish("metastores: 1\n")
}

async fn read_source() -> Result<Source, CliError> {
    if std::io::stdin().is_terminal() {
        return Err("metastore info needs a pqbench.lake-source on standard input".into());
    }
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut tokio::io::stdin(), &mut bytes).await?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err("metastore info reads a pqbench.lake-source document".into());
    }
    let source: Source = serde_json::from_slice(&bytes)
        .map_err(|error| format!("metastore info reads a pqbench.lake-source document: {error}"))?;
    if source.version != 1 {
        return Err(
            "unsupported lake source; expected kind `pqbench.lake-source` version 1".into(),
        );
    }
    if source.endpoint.trim().is_empty() {
        return Err("lake source needs an endpoint".into());
    }
    Ok(source)
}
