//! The walk's input: the environment's config and the parent-level refs.
//!
//! Every level is a stateless filter: it reads only its parent's records
//! (`catalog` reads `pqbench.catalog` refs, one per line) and writes its own
//! kind. Nothing else rides the stream. The endpoint, bearer, and dialect come
//! from the resolved configuration — `~/.pqbenchrc`, the environment, then the
//! flags (`pqbench setup` prints it for the shell to eval):
//!
//! ```text
//! PQB_ENDPOINT=… PQB_TOKEN=… pqbench metastore ls | pqbench catalog ls | …
//! ```
//!
//! `tee` (or `-o`) writes each level into the job tree; `info` stages emit the
//! same kind as the `ls` above them, so they enrich a stream in place.
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

/// The catalog, schema, and table a `pqbench.table-ref` ref names.
pub(crate) fn table_ref(
    command: &str,
    record: &Value,
) -> Result<(String, String, String), CliError> {
    let kind = record["kind"].as_str().unwrap_or_default();
    if kind != "pqbench.table-ref" {
        return Err(format!("expected pqbench.table-ref records, found {kind:?}").into());
    }
    if record["version"].as_u64() != Some(2) {
        return Err(
            format!("{command} reads pqbench.table-ref version 2; run `schema ls` first").into(),
        );
    }
    let id = record["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or("a pqbench.table-ref record needs an id")?;
    split_table(command, id)
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

/// The object-store options a ref carries (`AWS_*` names only). The process
/// environment supplies the rest through the client's own chain.
pub(crate) fn ref_env(record: &Value) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    if let Some(values) = record["env"].as_object() {
        for (key, value) in values {
            if let Some(value) = value.as_str() {
                env.insert(key.clone(), value.to_string());
            }
        }
    }
    env
}

/// The table's vended read credentials, by dialect: Unity's
/// `temporary-table-credentials`, or the Iceberg REST catalog's
/// `storage-credentials` for the table's data read.
pub(crate) async fn vend(
    context: &pqbench::Context,
    catalog: &str,
    schema: &str,
    name: &str,
) -> Result<Option<BTreeMap<String, String>>, CliError> {
    match context.config().dialect {
        pqbench::Dialect::Unity => {
            Ok(credentials::get::vend_unity(context, catalog, schema, name).await?)
        }
        pqbench::Dialect::Iceberg => {
            Ok(credentials::get::vend_iceberg(context, schema, name).await?)
        }
    }
}
