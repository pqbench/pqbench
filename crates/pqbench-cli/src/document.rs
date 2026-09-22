//! Documents on stdin or a file. `kind` decides what the command does.
//!
//! A producer resolves a table and pqbench measures bytes. Known kinds are
//! `pqbench.table` and `pqbench.remote-source`. Credentials stay on the
//! document so a pipe can carry them between processes.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Read, Write};
use std::path::Path;

use pqbench::table::TableInfo;
use serde::Deserialize;

use crate::CliError;

/// A versioned document naming what an external producer resolved, plus the
/// storage environment to read it with.
#[derive(Debug, Deserialize)]
pub(crate) struct RemoteSource {
    pub version: u32,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

/// One recognized stdin or file document.
pub(crate) enum Document {
    Table(TableInfo),
    RemoteSource(RemoteSource),
}

/// Read a document from `-` (stdin) or a file path.
pub(crate) fn read_document(input: &str) -> Result<Document, CliError> {
    if input == "-" {
        return parse_reader(std::io::stdin().lock());
    }
    parse_reader(std::fs::File::open(input)?)
}

/// Parse a JSON document from any reader.
pub(crate) fn parse_reader(mut reader: impl Read) -> Result<Document, CliError> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    parse_bytes(&bytes)
}

fn parse_bytes(bytes: &[u8]) -> Result<Document, CliError> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(invalid_json)?;
    let kind = value
        .get("kind")
        .and_then(|kind| kind.as_str())
        .ok_or_else(|| invalid_kind("document has no `kind`"))?;
    match kind {
        "pqbench.table" => {
            let table: TableInfo = serde_json::from_value(value).map_err(invalid_json)?;
            if table.version != 1 {
                return Err(
                    "unsupported table document; expected kind `pqbench.table` version 1".into(),
                );
            }
            Ok(Document::Table(table))
        }
        "pqbench.remote-source" => {
            let source: RemoteSource = serde_json::from_value(value).map_err(invalid_json)?;
            if source.version != 1 {
                return Err(
                    "unsupported source document; expected kind `pqbench.remote-source` version 1"
                        .into(),
                );
            }
            if source.inputs.is_empty() {
                return Err("source document contains no inputs".into());
            }
            Ok(Document::RemoteSource(source))
        }
        other => Err(invalid_kind(other)),
    }
}

/// Whether a path is `-` or a file whose first non-whitespace byte is `{`.
pub(crate) fn looks_like_json(path: &str) -> bool {
    if path == "-" {
        return true;
    }
    let path = Path::new(path);
    if !path.is_file() {
        return false;
    }
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut buf = [0u8; 64];
    let Ok(n) = file.read(&mut buf) else {
        return false;
    };
    buf[..n]
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'{')
}

/// Apply storage options carried by a document. Only `AWS_*` names are allowed.
pub(crate) fn apply_env(env: &BTreeMap<String, String>) -> Result<(), CliError> {
    for (key, value) in env {
        if !key.starts_with("AWS_") {
            return Err(format!("document may only set AWS_* variables, not `{key}`").into());
        }
        std::env::set_var(key, value);
    }
    Ok(())
}

/// Write a table document: pretty text on a terminal, full JSON on a pipe.
pub(crate) fn write_table(info: &TableInfo) -> Result<(), CliError> {
    let mut stdout = std::io::stdout().lock();
    if stdout.is_terminal() {
        write!(stdout, "{}", pqbench::table::render_text(info))?;
    } else {
        writeln!(stdout, "{}", pqbench::table::render_json(info)?)?;
    }
    Ok(())
}

fn invalid_json(error: serde_json::Error) -> CliError {
    format!(
        "invalid pqbench document at line {}, column {}",
        error.line(),
        error.column()
    )
    .into()
}

fn invalid_kind(kind: &str) -> CliError {
    format!("unsupported document kind `{kind}`; expected pqbench.table or pqbench.remote-source")
        .into()
}
