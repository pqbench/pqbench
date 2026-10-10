use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use futures_util::stream::StreamExt;
use pqbench::schema::{info, ls};
use serde::Serialize;
use serde_json::Value;

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
    /// also write the lz4 NDJSON stream to FILE
    #[arg(short = 'o', long = "output", value_name = "FILE")]
    output: Option<PathBuf>,
    /// requests in flight at once
    #[arg(long, default_value_t = 64)]
    fan_out: usize,
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

/// The row `schema info` writes for a schema.
fn schema_record(schema: &info::Schema) -> SchemaRecord<'_> {
    SchemaRecord {
        kind: "pqbench.schema",
        version: 1,
        catalog: &schema.catalog,
        name: &schema.name,
        comment: schema.comment.as_deref(),
        location: schema.location.as_deref(),
        properties: &schema.properties,
    }
}

/// The document `schema ls` writes, one line per table. Version 2 is the
/// walk's ref: a ref is durable data — no env — so a later stage
/// (`table info`, `credentials get`) gets the lake source's options on its
/// own stdin.
#[derive(Serialize)]
struct TableRefRecord<'a> {
    kind: &'static str,
    version: u32,
    id: &'a str,
    uri: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_path: Option<&'a str>,
}

impl Row for TableRefRecord<'_> {
    const HEADER: &'static [&'static str] = &["name", "uri", "storage path"];
    const ALIGN: &'static [Align] = &[Align::Left; 3];

    fn cells(&self) -> Vec<String> {
        vec![
            self.id.to_string(),
            self.uri.to_string(),
            self.storage_path.unwrap_or_default().to_string(),
        ]
    }
}

/// The row `schema ls` writes for a table.
fn table_record(table: &ls::TableRef) -> TableRefRecord<'_> {
    TableRefRecord {
        kind: "pqbench.table-ref",
        version: 2,
        id: &table.name,
        uri: &table.uri,
        storage_path: table.storage_path.as_deref(),
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
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err(
                "schema info takes CATALOG.SCHEMA or a pqbench.schema stream, not both".into(),
            );
        }
        let (catalog, name) = split_schema("schema info", schema)?;
        let record = info::read(
            &input.source.endpoint,
            &catalog,
            &name,
            input.source.token.as_deref(),
            input.source.table_format.into(),
        )
        .await?;
        emit.write_row(&schema_record(&record)).await?;
        return emit.finish("schemas: 1\n").await;
    }
    if !input.piped {
        return Err("schema info needs CATALOG.SCHEMA or a pqbench.schema stream".into());
    }
    let records = source::records("schema info", input.first, input.lines);
    let source = input.source;
    let mut schemas = 0;
    let mut reads = records
        .map(|record| async {
            let (catalog, name) = schema_ref(&record?)?;
            Ok::<_, CliError>(
                info::read(
                    &source.endpoint,
                    &catalog,
                    &name,
                    source.token.as_deref(),
                    source.table_format.into(),
                )
                .await?,
            )
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(record) = reads.next().await {
        emit.write_row(&schema_record(&record?)).await?;
        schemas += 1;
    }
    emit.finish(&format!("schemas: {schemas}\n")).await
}

async fn run_ls(args: &NameArgs) -> Result<(), CliError> {
    let input = read_input("schema ls").await?;
    let mut emit = Emitter::open(args.output.as_deref(), args.format.resolve(false))?;
    if let Some(schema) = &args.schema {
        if input.first.is_some() {
            return Err(
                "schema ls takes CATALOG.SCHEMA or a pqbench.schema stream, not both".into(),
            );
        }
        let (catalog, name) = split_schema("schema ls", schema)?;
        let tables = ls::list(
            &input.source.endpoint,
            &catalog,
            &name,
            input.source.token.as_deref(),
            input.source.table_format.into(),
        )
        .await?;
        for table in &tables {
            emit.write_row(&table_record(table)).await?;
        }
        return emit.finish(&format!("tables: {}\n", tables.len())).await;
    }
    if !input.piped {
        return Err("schema ls needs CATALOG.SCHEMA or a pqbench.schema stream".into());
    }
    let records = source::records("schema ls", input.first, input.lines);
    let source = input.source;
    let mut tables = 0;
    let mut lists = records
        .map(|record| async {
            let (catalog, name) = schema_ref(&record?)?;
            Ok::<_, CliError>(
                ls::list(
                    &source.endpoint,
                    &catalog,
                    &name,
                    source.token.as_deref(),
                    source.table_format.into(),
                )
                .await?,
            )
        })
        .buffer_unordered(args.fan_out.max(1));
    while let Some(listed) = lists.next().await {
        for table in listed? {
            emit.write_row(&table_record(&table)).await?;
            tables += 1;
        }
    }
    emit.finish(&format!("tables: {tables}\n")).await
}

/// The catalog and schema a `pqbench.schema` ref names.
fn schema_ref(record: &Value) -> Result<(String, String), CliError> {
    let kind = record["kind"].as_str().unwrap_or_default();
    if kind != "pqbench.schema" {
        return Err(format!("expected pqbench.schema records, found {kind:?}").into());
    }
    let catalog = record["catalog"]
        .as_str()
        .filter(|catalog| !catalog.is_empty())
        .ok_or("a pqbench.schema record needs a catalog")?;
    let name = record["name"]
        .as_str()
        .filter(|name| !name.is_empty())
        .ok_or("a pqbench.schema record needs a name")?;
    Ok((catalog.to_string(), name.to_string()))
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
