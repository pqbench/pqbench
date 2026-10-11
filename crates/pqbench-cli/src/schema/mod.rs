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

pub(crate) async fn run(workspace: &pqbench::Workspace, args: &SchemaArgs) -> Result<(), CliError> {
    match &args.command {
        SchemaCommand::Info(args) => info::run_info(workspace, args).await,
        SchemaCommand::Ls(args) => ls::run_ls(workspace, args).await,
    }
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
