//! Documents on stdin or a file. `kind` decides what the command does.
//!
//! A producer resolves a lake or a table and pqbench measures bytes. Known
//! kinds are `pqbench.lake`, `pqbench.table`, and `pqbench.remote-source`.
//! Credentials stay on the document so a pipe can carry them between processes.

use std::collections::BTreeMap;
use std::io::{IsTerminal, Read, Write};
use std::path::Path;

use pqbench::lake::Lake;
use pqbench::table::TableInfo;
use serde::Deserialize;

use crate::CliError;

/// Credentials for listing a Unity Catalog, OSS or Databricks. `endpoint` is
/// the server origin (`http://localhost:8080` or
/// `https://example.cloud.databricks.com`). `token` is the Databricks bearer
/// token; Unity OSS often has none. `env` is copied onto each listed table so
/// `pqbench table` can read its files.
#[derive(Deserialize)]
pub(crate) struct LakeSource {
    pub version: u32,
    pub endpoint: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

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
    Lake(Lake),
    LakeSource(LakeSource),
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
        "pqbench.lake" => {
            let lake: Lake = serde_json::from_value(value).map_err(invalid_json)?;
            if lake.version != 1 {
                return Err(
                    "unsupported lake document; expected kind `pqbench.lake` version 1".into(),
                );
            }
            if lake.tables.is_empty() {
                return Err("lake document contains no tables".into());
            }
            Ok(Document::Lake(lake))
        }
        "pqbench.lake-source" => {
            let source: LakeSource = serde_json::from_value(value).map_err(invalid_json)?;
            if source.version != 1 {
                return Err(
                    "unsupported lake source; expected kind `pqbench.lake-source` version 1".into(),
                );
            }
            if source.endpoint.trim().is_empty() {
                return Err("lake source needs an endpoint".into());
            }
            if source.env.keys().any(|key| !key.starts_with("AWS_")) {
                return Err("lake source env may only contain AWS_* names".into());
            }
            Ok(Document::LakeSource(source))
        }
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
    format!(
        "unsupported document kind `{kind}`; expected pqbench.lake, pqbench.lake-source, pqbench.table, or pqbench.remote-source"
    )
    .into()
}
