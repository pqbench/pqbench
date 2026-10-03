//! The walk's input: context and parent refs.
//!
//! A metadata command reads one stream. The first record may be a
//! `pqbench.lake-source` — the walk's context: the endpoint and bearer.
//! `PQB_ENDPOINT` / `PQB_TOKEN` fill in what the document leaves out. Every
//! other record is a parent-level ref (`catalog info` and `catalog ls` read
//! `pqbench.catalog` refs, one catalog per line), so the levels chain:
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
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

use crate::CliError;

/// The endpoint and bearer a metadata command runs under, the catalog
/// dialect it speaks, and the object-store options its table reads need.
#[derive(Debug, Clone)]
pub(crate) struct Source {
    pub endpoint: String,
    pub token: Option<String>,
    pub table_format: TableFormat,
    pub env: BTreeMap<String, String>,
}

/// The catalog dialect the walk runs against.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TableFormat {
    Unity,
    Iceberg,
}

impl From<TableFormat> for pqbench::catalog::ls::TableFormat {
    fn from(format: TableFormat) -> Self {
        match format {
            TableFormat::Unity => Self::Unity,
            TableFormat::Iceberg => Self::Iceberg,
        }
    }
}

impl From<TableFormat> for pqbench::schema::TableFormat {
    fn from(format: TableFormat) -> Self {
        match format {
            TableFormat::Unity => Self::Unity,
            TableFormat::Iceberg => Self::Iceberg,
        }
    }
}

impl From<TableFormat> for pqbench::tablev2::TableFormat {
    fn from(format: TableFormat) -> Self {
        match format {
            TableFormat::Unity => Self::Unity,
            TableFormat::Iceberg => Self::Iceberg,
        }
    }
}

/// One command's stdin: the resolved context and the parent records.
pub(crate) struct Input {
    pub source: Source,
    /// Whether stdin was a pipe (a stream), not a terminal.
    pub piped: bool,
    /// The first record, when it is a ref rather than the lake source.
    pub first: Option<Value>,
    /// The lines after the first record.
    pub lines: Lines<BufReader<tokio::io::Stdin>>,
}

/// The parent records: the peeked first record, then the rest of stdin.
///
/// A `pqbench.lake-source` after the first record is an error: the context
/// must come first for the stream to start.
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
            if record["kind"] == "pqbench.lake-source" {
                return Err(format!("{command} reads one pqbench.lake-source").into());
            }
            return Ok(Some((record, lines)));
        }
    });
    Box::pin(first.chain(rest))
}

#[derive(Deserialize)]
struct Document {
    version: u32,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    table_format: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
}

/// Read stdin's first record: a `pqbench.lake-source` (context) or a ref
/// (kept for the command's record stream).
pub(crate) async fn read_input(command: &'static str) -> Result<Input, CliError> {
    let piped = !std::io::stdin().is_terminal();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut document: Option<Document> = None;
    let mut first = None;
    if piped {
        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            let record: Value = serde_json::from_str(&line)
                .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
            if record["kind"] == "pqbench.lake-source" {
                document = Some(serde_json::from_value(record).map_err(|error| {
                    format!("{command} reads a pqbench.lake-source document: {error}")
                })?);
            } else {
                first = Some(record);
            }
            break;
        }
    }
    let source = resolve(command, document)?;
    Ok(Input {
        source,
        piped,
        first,
        lines,
    })
}

/// The document wins field by field; the environment fills in the rest.
fn resolve(command: &str, document: Option<Document>) -> Result<Source, CliError> {
    if let Some(document) = &document {
        if document.version != 1 {
            return Err(
                "unsupported lake source; expected kind `pqbench.lake-source` version 1".into(),
            );
        }
    }
    let endpoint = document
        .as_ref()
        .and_then(|document| document.endpoint.clone())
        .filter(|endpoint| !endpoint.trim().is_empty())
        .or_else(env_endpoint)
        .ok_or_else(|| {
            format!("{command} needs a pqbench.lake-source on standard input or PQB_ENDPOINT")
        })?;
    let token = document
        .as_ref()
        .and_then(|document| document.token.clone())
        .filter(|token| !token.is_empty())
        .or_else(env_token);
    let table_format = document
        .as_ref()
        .and_then(|document| document.table_format.clone())
        .or_else(env_table_format)
        .map(|value| parse_table_format(command, &value))
        .transpose()?
        .unwrap_or(TableFormat::Unity);
    let env = document.map(|document| document.env).unwrap_or_default();
    if let Some(key) = env.keys().find(|key| !key.starts_with("AWS_")) {
        return Err(format!("a lake source may only set AWS_* variables, not `{key}`").into());
    }
    Ok(Source {
        endpoint,
        token,
        table_format,
        env,
    })
}

fn env_endpoint() -> Option<String> {
    std::env::var("PQB_ENDPOINT")
        .ok()
        .filter(|endpoint| !endpoint.trim().is_empty())
}

fn env_token() -> Option<String> {
    std::env::var("PQB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
}

fn env_table_format() -> Option<String> {
    std::env::var("PQB_TABLE_FORMAT")
        .ok()
        .filter(|value| !value.is_empty())
}

/// `unity` (the default) or `iceberg`; the endpoint names the catalog base.
fn parse_table_format(command: &str, value: &str) -> Result<TableFormat, CliError> {
    match value {
        "unity" => Ok(TableFormat::Unity),
        "iceberg" => Ok(TableFormat::Iceberg),
        other => Err(format!(
            "{command}: PQB_TABLE_FORMAT expects `unity` or `iceberg`, got {other:?}"
        )
        .into()),
    }
}
