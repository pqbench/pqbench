use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use pqbench::schema::{info, ls};
use serde::Serialize;

use crate::emit::{Align, Emitter, Format, Row};
use crate::source::{self, read_input};
use crate::CliError;

/// Arguments for `schema`: one schema's record and the tables in it.
#[derive(Args)]
pub(crate) struct SchemaArgs {
    #[command(subcommand)]
    command: SchemaCommand,
}

#[derive(Subcommand)]
pub(crate) enum SchemaCommand {
    /// Show one schema's record
    Info(NameArgs),
    /// List the tables in a schema
    Ls(NameArgs),
}

/// Arguments for a schema subcommand: the `catalog.schema` name and the output
/// flags.
#[derive(Args)]
pub(crate) struct NameArgs {
    /// schema name (`catalog.schema`); without it, read `pqbench.schema` refs on stdin
    schema: Option<String>,
    /// stdout format: auto (table on a terminal) | table | json
    #[arg(long, value_enum, default_value_t = Format::Auto)]
    format: Format,
    /// also write the zstd NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
}

/// The document `schema info` writes.
#[derive(Serialize)]
struct SchemaRecord<'a> {
    kind: &'static str,
    version: u32,
    catalog: &'a str,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<&'a str>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    properties: &'a BTreeMap<String, String>,
}

impl Row for SchemaRecord<'_> {
    const HEADER: &'static [&'static str] = &["catalog", "name", "comment", "location"];
    const ALIGN: &'static [Align] = &[Align::Left; 4];

    fn cells(&self) -> Vec<String> {
        vec![
            self.catalog.to_string(),
            self.name.to_string(),
            self.comment.unwrap_or_default().to_string(),
            self.location.unwrap_or_default().to_string(),
        ]
    }
}

/// The document `schema ls` writes, one line per table.
#[derive(Serialize)]
struct TableRefRecord<'a> {
    kind: &'static str,
    version: u32,
    id: &'a str,
    uri: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<&'a str>,
}

impl Row for TableRefRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "uri", "format"];
    const ALIGN: &'static [Align] = &[Align::Left; 3];

    fn cells(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.uri.to_string(),
            self.format.unwrap_or_default().to_string(),
        ]
    }
}

pub(crate) async fn run(args: &SchemaArgs) -> Result<(), CliError> {
    match &args.command {
        SchemaCommand::Info(args) => run_info(args).await,
        SchemaCommand::Ls(args) => run_ls(args).await,
    }
}

async fn run_info(args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("schema info").await?;
    let refs = schema_refs("schema info", &input, args.schema.as_deref())?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    for (catalog, schema) in &refs {
        let record = info::read(
            &input.source.endpoint,
            catalog,
            schema,
            input.source.token.as_deref(),
            input.source.table_format.into(),
        )
        .await?;
        emit.write_row(&SchemaRecord {
            kind: "pqbench.schema",
            version: 1,
            catalog: &record.catalog,
            name: &record.name,
            comment: record.comment.as_deref(),
            location: record.location.as_deref(),
            properties: &record.properties,
        })?;
    }
    emit.finish(&format!("schemas: {}\n", refs.len()))
}

async fn run_ls(args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("schema ls").await?;
    let refs = schema_refs("schema ls", &input, args.schema.as_deref())?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    let mut tables = 0;
    for (catalog, schema) in &refs {
        for table in ls::list(
            &input.source.endpoint,
            catalog,
            schema,
            input.source.token.as_deref(),
            input.source.table_format.into(),
        )
        .await?
        {
            emit.write_row(&TableRefRecord {
                kind: "pqbench.table-ref",
                version: 1,
                id: &table.name,
                uri: &table.uri,
                format: table.format.as_deref(),
            })?;
            tables += 1;
        }
    }
    emit.finish(&format!("tables: {tables}\n"))
}

/// The schemas to read: the argument, or the `pqbench.schema` refs on stdin.
fn schema_refs(
    command: &str,
    input: &source::Input,
    schema: Option<&str>,
) -> Result<Vec<(String, String)>, CliError> {
    if let Some(schema) = schema {
        if !input.items.is_empty() {
            return Err(format!(
                "{command} takes CATALOG.SCHEMA or a pqbench.schema stream, not both"
            )
            .into());
        }
        return Ok(vec![split_schema(command, schema)?]);
    }
    if !input.piped {
        return Err(format!("{command} needs CATALOG.SCHEMA or a pqbench.schema stream").into());
    }
    let mut refs = Vec::new();
    for item in &input.items {
        let kind = item["kind"].as_str().unwrap_or_default();
        if kind != "pqbench.schema" {
            return Err(format!("expected pqbench.schema records, found {kind:?}").into());
        }
        let catalog = item["catalog"]
            .as_str()
            .filter(|catalog| !catalog.is_empty())
            .ok_or("a pqbench.schema record needs a catalog")?;
        let name = item["name"]
            .as_str()
            .filter(|name| !name.is_empty())
            .ok_or("a pqbench.schema record needs a name")?;
        refs.push((catalog.to_string(), name.to_string()));
    }
    Ok(refs)
}

/// Split `catalog.schema` at the first dot; an Iceberg namespace keeps its
/// remaining dots.
fn split_schema(command: &str, fqn: &str) -> Result<(String, String), CliError> {
    let Some((catalog, schema)) = fqn.split_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA; got {fqn:?}").into());
    };
    if catalog.is_empty() || schema.is_empty() {
        return Err(format!("{command} takes CATALOG.SCHEMA; got {fqn:?}").into());
    }
    Ok((catalog.to_string(), schema.to_string()))
}
