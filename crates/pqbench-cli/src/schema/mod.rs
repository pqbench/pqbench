//! `schema`: one schema's record, or the tables in it.

use std::path::PathBuf;

use clap::{Args, Subcommand};
use serde_json::Value;

use crate::emit::Format;
use crate::CliError;

mod info;
mod ls;

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
    /// schema name (`catalog.schema`); without it, read schema refs on stdin
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

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &SchemaArgs) -> Result<(), CliError> {
    match &args.command {
        SchemaCommand::Info(args) => info::run(workspace, args).await,
        SchemaCommand::Ls(args) => ls::run(workspace, args).await,
    }
}

/// The catalog and schema a schema ref names: a
/// `catalogs/{catalog}/schemas/{schema}` resource name.
fn schema_ref(record: &Value) -> Result<(String, String), CliError> {
    record["name"]
        .as_str()
        .and_then(pqbench::v1::name::parse_schema)
        .ok_or_else(|| {
            "expected a schema resource name (`catalogs/{catalog}/schemas/{schema}`)".into()
        })
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
