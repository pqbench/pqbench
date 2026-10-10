//! Documents on stdin or a file. `kind` decides what the command does.
//!
//! A producer resolves a table and pqbench measures bytes. Known kinds are
//! `pqbench.table-file`, `pqbench.remote-source`, `pqbench.bytemass-file`, and
//! `pqbench.bytemass-row`. A pipe writes NDJSON; every record carries a table
//! `id` so rows stay attributable. A terminal prints an aligned table; a pipe
//! streams NDJSON (override with `--format`). `-o` also writes the lz4 NDJSON
//! stream. Credentials stay on the document so a pipe can carry them between
//! processes.

use std::collections::BTreeMap;
use std::ops::AsyncFnMut;

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use pqbench::bytemass::MassRow;
use pqbench::table::TableFile;
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

/// One JSON value from a table-file stream or a one-object document.
pub(crate) enum Record {
    /// One file of a partition, with the env to read it.
    File {
        id: String,
        file: TableFile,
        env: BTreeMap<String, String>,
    },
    RemoteSource(RemoteSource),
    BytemassFile(pqbench::bytemass::FileStat),
    BytemassRow {
        id: String,
        row: MassRow,
    },
    BytemassPage,
}

/// Call `visit` once per JSON value, as soon as that value is complete.
///
/// `-` streams standard input line by line. A path is read whole — documents
/// are metadata, not data — and an lz4 frame is decoded first.
pub(crate) async fn visit_input<F>(input: &str, visit: F) -> Result<(), CliError>
where
    F: AsyncFnMut(Record) -> Result<(), CliError>,
{
    if input == "-" {
        visit_stdin(visit).await
    } else {
        visit_file(input, visit).await
    }
}

/// Read NDJSON from standard input, one value at a time.
async fn visit_stdin<F>(mut visit: F) -> Result<(), CliError>
where
    F: AsyncFnMut(Record) -> Result<(), CliError>,
{
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut buffer = String::new();
    let mut empty = true;
    while let Some(line) = lines.next_line().await? {
        buffer.push_str(&line);
        buffer.push('\n');
        match serde_json::from_str::<serde_json::Value>(&buffer) {
            Ok(value) => {
                empty = false;
                visit(classify(value)?).await?;
                buffer.clear();
            }
            Err(error) if error.is_eof() => {}
            Err(error) => return Err(invalid_json(error)),
        }
    }
    if !buffer.trim().is_empty() {
        return Err("incomplete document".into());
    }
    if empty {
        return Err("empty document".into());
    }
    Ok(())
}

/// Read a document file whole, decoding an lz4 frame first.
async fn visit_file<F>(input: &str, mut visit: F) -> Result<(), CliError>
where
    F: AsyncFnMut(Record) -> Result<(), CliError>,
{
    let bytes = tokio::fs::read(input).await?;
    let bytes = if bytes.starts_with(&LZ4_MAGIC) {
        decode_lz4(&bytes)?
    } else {
        bytes
    };
    let values = serde_json::Deserializer::from_reader(std::io::Cursor::new(bytes));
    let mut empty = true;
    for value in values.into_iter::<serde_json::Value>() {
        empty = false;
        visit(classify(value.map_err(invalid_json)?)?).await?;
    }
    if empty {
        return Err("empty document".into());
    }
    Ok(())
}

fn classify(value: serde_json::Value) -> Result<Record, CliError> {
    let kind = value
        .get("kind")
        .and_then(|kind| kind.as_str())
        .ok_or("document has no `kind`")?
        .to_string();
    match kind.as_str() {
        "pqbench.table-file" => {
            let (id, file, env) = parse_file(value)?;
            Ok(Record::File { id, file, env })
        }
        "pqbench.remote-source" => Ok(Record::RemoteSource(parse_remote(value)?)),
        "pqbench.bytemass-file" => Ok(Record::BytemassFile(
            serde_json::from_value(value).map_err(invalid_json)?,
        )),
        "pqbench.bytemass-page" => Ok(Record::BytemassPage),
        "pqbench.bytemass-row" => {
            let (id, row) = parse_mass_row(value)?;
            Ok(Record::BytemassRow { id, row })
        }
        other => Err(invalid_kind(other)),
    }
}

fn parse_mass_row(value: serde_json::Value) -> Result<(String, MassRow), CliError> {
    #[derive(Deserialize)]
    struct Wire {
        #[serde(default)]
        id: String,
        #[serde(flatten)]
        row: MassRow,
    }
    let wire = serde_json::from_value::<Wire>(value).map_err(invalid_json)?;
    Ok((wire.id, wire.row))
}

fn parse_file(
    value: serde_json::Value,
) -> Result<(String, TableFile, BTreeMap<String, String>), CliError> {
    #[derive(Deserialize)]
    struct Wire {
        #[serde(default)]
        id: String,
        #[serde(default)]
        env: BTreeMap<String, String>,
        #[serde(flatten)]
        file: TableFile,
    }
    let wire = serde_json::from_value::<Wire>(value).map_err(invalid_json)?;
    ensure_aws_env(&wire.env)?;
    Ok((wire.id, wire.file, wire.env))
}

fn parse_remote(value: serde_json::Value) -> Result<RemoteSource, CliError> {
    let source: RemoteSource = serde_json::from_value(value).map_err(invalid_json)?;
    if source.version != 1 {
        return Err(
            "unsupported source document; expected kind `pqbench.remote-source` version 1".into(),
        );
    }
    if source.inputs.is_empty() {
        return Err("source document contains no inputs".into());
    }
    ensure_aws_env(&source.env)?;
    Ok(source)
}

fn ensure_aws_env(env: &BTreeMap<String, String>) -> Result<(), CliError> {
    if let Some(key) = env.keys().find(|key| !key.starts_with("AWS_")) {
        return Err(format!("document may only set AWS_* variables, not `{key}`").into());
    }
    Ok(())
}

const LZ4_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

/// Decode an lz4 frame whole; documents are metadata, not data.
fn decode_lz4(bytes: &[u8]) -> Result<Vec<u8>, CliError> {
    let mut decoder = lz4::Decoder::new(std::io::Cursor::new(bytes))?;
    let mut decoded = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut decoded)?;
    Ok(decoded)
}

/// Whether a path is `-`, JSON (`{`), or an lz4 frame.
pub(crate) async fn is_document(path: &str) -> bool {
    if path == "-" {
        return true;
    }
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return false;
    };
    let mut buf = [0u8; 64];
    let Ok(n) = file.read(&mut buf).await else {
        return false;
    };
    if n >= 4 && buf[..4] == LZ4_MAGIC {
        return true;
    }
    buf[..n]
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'{')
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
    format!("unsupported document kind `{kind}`; expected pqbench.table-file, pqbench.remote-source, pqbench.bytemass-file, or pqbench.bytemass-row")
        .into()
}
