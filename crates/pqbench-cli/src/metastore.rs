use std::path::PathBuf;

use clap::{Args, Subcommand};
use pqbench::metastore::{info, ls};
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
use crate::source::read_input;
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
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
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
    let input = read_input("metastore info").await?;
    let metastore = info::read(&input.source.endpoint, input.source.token.as_deref()).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    emit.write_row(&MetastoreRecord {
        kind: "pqbench.metastore",
        version: 1,
        name: &metastore.name,
        id: &metastore.id,
        cloud: &metastore.cloud,
        region: &metastore.region,
    })
    .await?;
    emit.finish("metastores: 1\n").await
}

async fn run_ls(args: &OutputArgs) -> Result<(), CliError> {
    let input = read_input("metastore ls").await?;
    let catalogs = ls::list(&input.source.endpoint, input.source.token.as_deref()).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    for catalog in &catalogs {
        emit.write_row(&CatalogRecord {
            kind: "pqbench.catalog",
            version: 1,
            name: &catalog.name,
            catalog_type: catalog.catalog_type.as_deref(),
        })
        .await?;
    }
    emit.finish(&format!("catalogs: {}\n", catalogs.len()))
        .await
}
