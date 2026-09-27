use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::future::join_all;
use pqbench::catalog::{info, ls};
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
use crate::source::{self, read_input};
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
    /// catalog name; without it, read `pqbench.catalog` refs on stdin
    catalog: Option<String>,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the lz4 NDJSON stream to FILE
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
    let input = read_input("catalog info").await?;
    let names = catalog_names("catalog info", &input, args.catalog.as_deref())?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let reads = names
        .iter()
        .map(|name| info::read(&input.source.endpoint, name, input.source.token.as_deref()));
    for catalog in join_all(reads).await {
        let catalog = catalog?;
        emit.write_row(&CatalogRecord {
            kind: "pqbench.catalog",
            version: 1,
            name: &catalog.name,
            catalog_type: catalog.catalog_type.as_deref(),
            comment: catalog.comment.as_deref(),
            owner: catalog.owner.as_deref(),
        })
        .await?;
    }
    emit.finish(&format!("catalogs: {}\n", names.len())).await
}

async fn run_ls(args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("catalog ls").await?;
    let names = catalog_names("catalog ls", &input, args.catalog.as_deref())?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let lists = names.iter().map(|name| {
        ls::list(
            &input.source.endpoint,
            name,
            input.source.token.as_deref(),
            input.source.table_format.into(),
        )
    });
    let mut schemas = 0;
    for listed in join_all(lists).await {
        for schema in listed? {
            emit.write_row(&SchemaRecord {
                kind: "pqbench.schema",
                version: 1,
                catalog: &schema.catalog,
                name: &schema.name,
            })
            .await?;
            schemas += 1;
        }
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}

/// The catalogs to read: the argument, or the `pqbench.catalog` refs on stdin.
fn catalog_names(
    command: &str,
    input: &source::Input,
    catalog: Option<&str>,
) -> Result<Vec<String>, CliError> {
    if let Some(catalog) = catalog {
        if !input.items.is_empty() {
            return Err(
                format!("{command} takes CATALOG or a pqbench.catalog stream, not both").into(),
            );
        }
        return Ok(vec![catalog.to_string()]);
    }
    if !input.piped {
        return Err(format!("{command} needs CATALOG or a pqbench.catalog stream").into());
    }
    source::names(&input.items, "pqbench.catalog")
}
