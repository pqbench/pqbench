//! The walk's input: the environment's config and the parent-level refs.
//!
//! Every level is a stateless filter: it reads only its parent's records
//! (`list schemas` reads `Catalog` names, one per line) and writes its own. The
//! endpoint, bearer, and dialect come from the resolved configuration —
//! `~/.pqbenchrc`, the environment, then the flags (`pqbench setup` prints it
//! for the shell to eval):
//!
//! ```text
//! PQB_ENDPOINT=… PQB_TOKEN=… pqbench list catalogs | pqbench list schemas | …
//! ```
//!
//! `tee` (or `-o`) writes each level into the job tree; `get` stages emit the
//! same resource as the `list` they read from, so they enrich a stream in
//! place.
//!
//! Records are read one at a time as the command consumes them, so a level
//! never buffers its parent: a slow consumer stops reading, and the pipe
//! backpressures the producer.

use std::collections::BTreeMap;
use std::io::IsTerminal;

use futures_util::stream::{self, Stream, StreamExt};
use pqbench::credentials;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

use crate::CliError;

/// The parent records on standard input: whether stdin was a pipe (a stream),
/// the peeked first record, and the rest of the lines.
pub(crate) struct Input {
    /// Whether stdin was a pipe (a stream), not a terminal.
    pub piped: bool,
    /// The first record, when stdin carried one.
    pub first: Option<Value>,
    /// The lines after the first record.
    pub lines: Lines<BufReader<tokio::io::Stdin>>,
}

/// The parent records: the peeked first record, then the rest of stdin.
pub(crate) fn records(
    command: &'static str,
    first: Option<Value>,
    lines: Lines<BufReader<tokio::io::Stdin>>,
) -> impl Stream<Item = Result<Value, CliError>> + Unpin {
    let first = stream::iter(first).map(Ok::<_, CliError>);
    let rest = stream::try_unfold(lines, move |mut lines| async move {
        loop {
            let Some(line) = lines.next_line().await? else {
                return Ok(None);
            };
            if line.trim().is_empty() {
                continue;
            }
            let record: Value = serde_json::from_str(&line)
                .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
            return Ok(Some((record, lines)));
        }
    });
    Box::pin(first.chain(rest))
}

/// Read stdin's first record (a parent ref, kept for the command's stream),
/// without resolving config.
pub(crate) async fn read_input(command: &'static str) -> Result<Input, CliError> {
    let piped = !std::io::stdin().is_terminal();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut first = None;
    if piped {
        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let record: Value = serde_json::from_str(&line)
                .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
            first = Some(record);
            break;
        }
    }
    Ok(Input {
        piped,
        first,
        lines,
    })
}

/// The metastore id a metastore ref names: a `metastores/{metastore}` resource
/// name. The metastore is a singleton, so the value only validates the input.
pub(crate) fn metastore_ref(command: &str, record: &Value) -> Result<String, CliError> {
    record["name"]
        .as_str()
        .and_then(pqbench::v1::name::parse_metastore)
        .ok_or_else(|| {
            format!(
                "{command} reads pqbench metastore resources; a record needs a `metastores/…` name"
            )
            .into()
        })
}

/// The catalog id a catalog ref names: a `catalogs/{catalog}` resource name.
pub(crate) fn catalog_ref(command: &str, record: &Value) -> Result<String, CliError> {
    record["name"]
        .as_str()
        .and_then(pqbench::v1::name::parse_catalog)
        .ok_or_else(|| {
            format!("{command} reads pqbench catalog resources; a record needs a `catalogs/…` name")
                .into()
        })
}

/// The catalog and schema a schema ref names: a
/// `catalogs/{catalog}/schemas/{schema}` resource name.
pub(crate) fn schema_ref(command: &str, record: &Value) -> Result<(String, String), CliError> {
    record["name"]
        .as_str()
        .and_then(pqbench::v1::name::parse_schema)
        .ok_or_else(|| {
            format!(
                "{command} reads pqbench schema resources; a record needs a `catalogs/…/schemas/…` name"
            )
            .into()
        })
}

/// Split `catalog.schema` at the first dot; an Iceberg namespace keeps its
/// remaining dots.
pub(crate) fn split_schema(command: &str, fqn: &str) -> Result<(String, String), CliError> {
    let Some((catalog, schema)) = fqn.split_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA; got {fqn:?}").into());
    };
    if catalog.is_empty() || schema.is_empty() {
        return Err(format!("{command} takes CATALOG.SCHEMA; got {fqn:?}").into());
    }
    Ok((catalog.to_string(), schema.to_string()))
}

/// The catalog, schema, and table a table ref names: a
/// `catalogs/{catalog}/schemas/{schema}/tables/{table}` resource name.
pub(crate) fn table_ref(
    command: &str,
    record: &Value,
) -> Result<(String, String, String), CliError> {
    record["name"]
        .as_str()
        .and_then(pqbench::v1::name::parse_table)
        .ok_or_else(|| {
            format!(
                "{command} reads pqbench table resources; a record needs a `catalogs/…/tables/…` name"
            )
            .into()
        })
}

/// The storage location a table ref names: the [`Table`]'s `uri`. `None` when
/// the ref names no location (a view, or an Iceberg identifier until
/// `get table` resolves it).
///
/// [`Table`]: pqbench::v1::Table
pub(crate) fn ref_storage(record: &Value) -> Option<&str> {
    record["uri"].as_str().filter(|value| !value.is_empty())
}

/// Split `catalog.schema.table` at the first and last dots; an Iceberg
/// namespace keeps its remaining dots.
pub(crate) fn split_table(command: &str, fqn: &str) -> Result<(String, String, String), CliError> {
    let Some((catalog, rest)) = fqn.split_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    };
    let Some((schema, table)) = rest.rsplit_once('.') else {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    };
    if catalog.is_empty() || schema.is_empty() || table.is_empty() {
        return Err(format!("{command} takes CATALOG.SCHEMA.TABLE; got {fqn:?}").into());
    }
    Ok((catalog.to_string(), schema.to_string(), table.to_string()))
}

/// The storage lease a ref carries: the proto `Table.credentials` value, or the
/// older `env` bag. The credentials win, so a table-scoped lease replaces any
/// static key for that table.
pub(crate) fn ref_env(record: &Value) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    if let Some(values) = record["env"].as_object() {
        for (key, value) in values {
            if let Some(value) = value.as_str() {
                env.insert(key.clone(), value.to_string());
            }
        }
    }
    if let Ok(credentials) =
        serde_json::from_value::<pqbench::v1::Credentials>(record["credentials"].clone())
    {
        env.extend(credentials.to_env());
    }
    env
}

/// The table's vended read credentials, by dialect: Unity's
/// `temporary-table-credentials`, or the Iceberg REST catalog's
/// `storage-credentials` for the table's data read.
pub(crate) async fn vend(
    workspace: &pqbench::Workspace,
    catalog: &str,
    schema: &str,
    name: &str,
) -> Result<Option<BTreeMap<String, String>>, CliError> {
    match workspace.config().dialect {
        pqbench::Dialect::Unity => {
            Ok(credentials::get::vend_unity(workspace, catalog, schema, name).await?)
        }
        pqbench::Dialect::Iceberg => {
            Ok(credentials::get::vend_iceberg(workspace, schema, name).await?)
        }
    }
}
