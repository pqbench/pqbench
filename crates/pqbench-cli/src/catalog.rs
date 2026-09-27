use std::path::PathBuf;

use clap::{Args, Subcommand};
use pqbench::catalog::{info, ls};
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
use crate::source::read_source;
use crate::CliError;

/// Arguments for `catalog`: one catalog's record.
#[derive(Args)]
pub(crate) struct CatalogArgs {
    #[command(subcommand)]
    command: CatalogCommand,
}

#[derive(Subcommand)]
pub(crate) enum CatalogCommand {
    /// Show one catalog's record
    Info(NameArgs),
    /// List the schemas in a catalog
    Ls(NameArgs),
}

/// Arguments for a catalog subcommand: the catalog name and the output flags.
#[derive(Args)]
pub(crate) struct NameArgs {
    /// catalog name (Unity `catalog`)
    catalog: String,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the zstd NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

/// The document `catalog info` writes.
#[derive(Serialize)]
struct CatalogRecord<'a> {
    kind: &'static str,
    version: u32,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_type: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<&'a str>,
}

impl Row for CatalogRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "catalog_type", "comment", "owner"];
    const ALIGN: &'static [Align] = &[Align::Left; 4];

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.to_string(),
            self.catalog_type.unwrap_or_default().to_string(),
            self.comment.unwrap_or_default().to_string(),
            self.owner.unwrap_or_default().to_string(),
        ]
    }
}

/// The document `catalog ls` writes, one line per schema.
#[derive(Serialize)]
struct SchemaRecord<'a> {
    kind: &'static str,
    version: u32,
    catalog: &'a str,
    name: &'a str,
}

impl Row for SchemaRecord<'_> {
    const HEADER: &'static [&'static str] = &["catalog", "name"];
    const ALIGN: &'static [Align] = &[Align::Left; 2];

    fn cells(&self) -> Vec<String> {
        vec![self.catalog.to_string(), self.name.to_string()]
    }
}

pub(crate) async fn run(args: &CatalogArgs) -> Result<(), CliError> {
    match &args.command {
        CatalogCommand::Info(args) => run_info(args).await,
        CatalogCommand::Ls(args) => run_ls(args).await,
    }
}

async fn run_info(args: &NameArgs) -> Result<(), CliError> {
    let source = read_source("catalog").await?;
    let catalog = info::read(&source.endpoint, &args.catalog, source.token.as_deref()).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    emit.write_row(&CatalogRecord {
        kind: "pqbench.catalog",
        version: 1,
        name: &catalog.name,
        catalog_type: catalog.catalog_type.as_deref(),
        comment: catalog.comment.as_deref(),
        owner: catalog.owner.as_deref(),
    })?;
    emit.finish("catalogs: 1\n")
}

async fn run_ls(args: &NameArgs) -> Result<(), CliError> {
    let source = read_source("catalog").await?;
    let schemas = ls::list(&source.endpoint, &args.catalog, source.token.as_deref()).await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    for schema in &schemas {
        emit.write_row(&SchemaRecord {
            kind: "pqbench.schema",
            version: 1,
            catalog: &schema.catalog,
            name: &schema.name,
        })?;
    }
    emit.finish(&format!("schemas: {}\n", schemas.len()))
}
