use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::StreamExt;
use pqbench::catalog::{info, ls};
use serde::Serialize;
use serde_json::Value;

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
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
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

/// The row `catalog info` writes for a catalog.
fn catalog_record(catalog: &info::Catalog) -> CatalogRecord<'_> {
    CatalogRecord {
        kind: "pqbench.catalog",
        version: 1,
        name: &catalog.name,
        catalog_type: catalog.catalog_type.as_deref(),
        comment: catalog.comment.as_deref(),
        owner: catalog.owner.as_deref(),
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

/// The row `catalog ls` writes for a schema.
fn schema_record(schema: &ls::Schema) -> SchemaRecord<'_> {
    SchemaRecord {
        kind: "pqbench.schema",
        version: 1,
        catalog: &schema.catalog,
        name: &schema.name,
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
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(name) = &args.catalog {
        if input.first.is_some() {
            return Err("catalog info takes CATALOG or a pqbench.catalog stream, not both".into());
        }
        let catalog =
            info::read(&input.source.endpoint, name, input.source.token.as_deref()).await?;
        emit.write_row(&catalog_record(&catalog)).await?;
        return emit.finish("catalogs: 1\n").await;
    }
    if !input.piped {
        return Err("catalog info needs CATALOG or a pqbench.catalog stream".into());
    }
    let records = source::records("catalog info", input.first, input.lines);
    let source = input.source;
    let mut catalogs = 0;
    let mut reads = records
        .map(|record| async {
            let name = catalog_name(&record?)?;
            Ok::<_, CliError>(info::read(&source.endpoint, &name, source.token.as_deref()).await?)
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(catalog) = reads.next().await {
        emit.write_row(&catalog_record(&catalog?)).await?;
        catalogs += 1;
    }
    emit.finish(&format!("catalogs: {catalogs}\n")).await
}

async fn run_ls(args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("catalog ls").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(name) = &args.catalog {
        if input.first.is_some() {
            return Err("catalog ls takes CATALOG or a pqbench.catalog stream, not both".into());
        }
        let schemas = ls::list(
            &input.source.endpoint,
            name,
            input.source.token.as_deref(),
            input.source.table_format.into(),
        )
        .await?;
        for schema in &schemas {
            emit.write_row(&schema_record(schema)).await?;
        }
        return emit.finish(&format!("schemas: {}\n", schemas.len())).await;
    }
    if !input.piped {
        return Err("catalog ls needs CATALOG or a pqbench.catalog stream".into());
    }
    let records = source::records("catalog ls", input.first, input.lines);
    let source = input.source;
    let mut schemas = 0;
    let mut lists = records
        .map(|record| async {
            let name = catalog_name(&record?)?;
            Ok::<_, CliError>(
                ls::list(
                    &source.endpoint,
                    &name,
                    source.token.as_deref(),
                    source.table_format.into(),
                )
                .await?,
            )
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(listed) = lists.next().await {
        for schema in listed? {
            emit.write_row(&schema_record(&schema)).await?;
            schemas += 1;
        }
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}

/// The catalog name a `pqbench.catalog` ref names.
fn catalog_name(record: &Value) -> Result<String, CliError> {
    let kind = record["kind"].as_str().unwrap_or_default();
    if kind != "pqbench.catalog" {
        return Err(format!("expected pqbench.catalog records, found {kind:?}").into());
    }
    let name = record["name"]
        .as_str()
        .filter(|name| !name.is_empty())
        .ok_or("a pqbench.catalog record needs a name")?;
    Ok(name.to_string())
}
