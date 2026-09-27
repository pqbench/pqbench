//! The walk's input: context and parent refs.
//!
//! A metadata command reads one stream. A `pqbench.lake-source` record is the
//! walk's context — the endpoint and bearer — and `PQB_ENDPOINT` /
//! `PQB_TOKEN` fill in what the document leaves out. Every other record is a
//! parent-level ref (`catalog info` and `catalog ls` read `pqbench.catalog`
//! refs, one catalog per line), so the levels chain:
//!
//! ```text
//! PQB_ENDPOINT=… PQB_TOKEN=… pqbench metastore ls | pqbench catalog ls | …
//! ```
//!
//! `tee` (or `-o`) writes each level into the job tree; `info` stages emit the
//! same kind as the `ls` above them, so they enrich a stream in place.

use std::io::IsTerminal;

use serde::Deserialize;
use serde_json::Value;

use crate::CliError;

/// The endpoint and bearer a metadata command runs under.
#[derive(Debug, Clone)]
pub(crate) struct Source {
    pub endpoint: String,
    pub token: Option<String>,
}

/// One command's stdin: the resolved context and the parent's records.
pub(crate) struct Input {
    pub source: Source,
    pub items: Vec<Value>,
    /// Whether stdin was a pipe (a stream), not a terminal.
    pub piped: bool,
}

#[derive(Deserialize)]
struct Document {
    version: u32,
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    token: Option<String>,
}

/// Read stdin: a `pqbench.lake-source` (context) and/or parent refs (items).
pub(crate) async fn read_input(command: &str) -> Result<Input, CliError> {
    let piped = !std::io::stdin().is_terminal();
    let mut document: Option<Document> = None;
    let mut items = Vec::new();
    if piped {
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut tokio::io::stdin(), &mut bytes).await?;
        for line in bytes.split(|byte| *byte == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let record: Value = serde_json::from_slice(line)
                .map_err(|error| format!("{command} reads NDJSON records: {error}"))?;
            if record["kind"] == "pqbench.lake-source" {
                if document.is_some() {
                    return Err(format!("{command} reads one pqbench.lake-source").into());
                }
                document = Some(serde_json::from_value(record).map_err(|error| {
                    format!("{command} reads a pqbench.lake-source document: {error}")
                })?);
            } else {
                items.push(record);
            }
        }
    }
    let source = resolve(command, document)?;
    Ok(Input {
        source,
        items,
        piped,
    })
}

/// The `name` of every record of `kind`; a record of another kind is an error.
pub(crate) fn names(items: &[Value], kind: &str) -> Result<Vec<String>, CliError> {
    let mut names = Vec::new();
    for item in items {
        let found = item["kind"].as_str().unwrap_or_default();
        if found != kind {
            return Err(format!("expected {kind} records, found {found:?}").into());
        }
        let name = item["name"]
            .as_str()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| format!("a {kind} record needs a name"))?;
        names.push(name.to_string());
    }
    Ok(names)
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
    Ok(Source { endpoint, token })
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
