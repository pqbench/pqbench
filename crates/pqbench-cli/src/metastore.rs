use std::io::IsTerminal;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use pqbench::metastore::{info, ls};
use serde::{Deserialize, Serialize};

use crate::emit::{Align, Emitter, Format, Row};
use crate::CliError;

/// Arguments for `metastore`: the endpoint's metastore and its catalogs.
#[derive(Args)]
pub(crate) struct MetastoreArgs {
    #[command(subcommand)]
    command: MetastoreCommand,
}

#[derive(Subcommand)]
pub(crate) enum MetastoreCommand {
    /// Show the endpoint's metastore record
    Info(OutputArgs),
    /// List the catalogs at the endpoint
    Ls(OutputArgs),
}

#[derive(Args)]
pub(crate) struct OutputArgs {
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the zstd NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

/// The document the metastore commands read: a `pqbench.lake-source`.
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

/// The document `metastore ls` writes, one line per catalog.
#[derive(Serialize)]
struct CatalogRecord<'a> {
    kind: &'static str,
    version: u32,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_type: Option<&'a str>,
}

impl Row for CatalogRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "catalog_type"];
    const ALIGN: &'static [Align] = &[Align::Left; 2];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.to_string(),
            self.catalog_type.unwrap_or_default().to_string(),
        ]
    }
}

pub(crate) async fn run(args: &MetastoreArgs) -> Result<(), CliError> {
    match &args.command {
        MetastoreCommand::Info(args) => run_info(args).await,
        MetastoreCommand::Ls(args) => run_ls(args).await,
    }
}

async fn run_info(args: &OutputArgs) -> Result<(), CliError> {
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

async fn run_ls(args: &OutputArgs) -> Result<(), CliError> {
    let source = read_source().await?;
    let catalogs = ls::list(&source.endpoint, source.token.as_deref()).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    for catalog in &catalogs {
        emit.write_row(&CatalogRecord {
            kind: "pqbench.catalog",
            version: 1,
            name: &catalog.name,
            catalog_type: catalog.catalog_type.as_deref(),
        })?;
    }
    emit.finish(&format!("catalogs: {}\n", catalogs.len()))
}

async fn read_source() -> Result<Source, CliError> {
    if std::io::stdin().is_terminal() {
        return Err("metastore needs a pqbench.lake-source on standard input".into());
    }
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut tokio::io::stdin(), &mut bytes).await?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err("metastore reads a pqbench.lake-source document".into());
    }
    let source: Source = serde_json::from_slice(&bytes)
        .map_err(|error| format!("metastore reads a pqbench.lake-source document: {error}"))?;
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
