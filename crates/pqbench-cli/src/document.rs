//! Documents on stdin or a file. `kind` decides what the command does.
//!
//! A producer resolves a table and pqbench measures bytes. Known kinds are
//! `pqbench.table`, `pqbench.table-ref`, and `pqbench.remote-source`. A pipe
//! writes NDJSON; every record carries a table `id` so many tables can mix.
//! A terminal prints a short summary and requires `-o` (zstd NDJSON).
//! A single `pqbench.table` object is still accepted. Credentials stay on
//! the document so a pipe can carry them between processes.

use std::collections::BTreeMap;
use std::ops::AsyncFnMut;

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use crate::emit::Emit;

use pqbench::table::{LogCommit, TableFile, TableFormat, TableInfo};
use serde::{Deserialize, Serialize};

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

/// One JSON value from a table stream or a one-object document.
pub(crate) enum Record {
    /// A table to load (`lake` emits these; `table` fans them out).
    TableRef(TableRef),
    Begin(Begin),
    #[allow(dead_code)]
    Log {
        id: String,
        commit: LogCommit,
    },
    File {
        id: String,
        file: TableFile,
    },
    End {
        id: String,
    },
    Table(TableInfo),
    RemoteSource(RemoteSource),
}

/// One table name for `table` to load. `id` tags every later line.
#[derive(Debug, Clone)]
pub(crate) struct TableRef {
    pub id: String,
    pub uri: String,
    pub env: BTreeMap<String, String>,
}

/// Header of an NDJSON table stream. `log` and `files` follow as later lines.
pub(crate) struct Begin {
    pub id: String,
    pub env: BTreeMap<String, String>,
}

/// Call `visit` once per JSON value, as soon as that value is complete.
///
/// `-` streams standard input line by line. A path is read whole — documents
/// are metadata, not data — and a zstd frame is decoded first.
pub(crate) async fn visit_input<F>(input: &str, mut visit: F) -> Result<(), CliError>
where
    F: AsyncFnMut(Record) -> Result<(), CliError>,
{
    if input == "-" {
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
        return Ok(());
    }

    let bytes = tokio::fs::read(input).await?;
    let bytes = if bytes.starts_with(&ZSTD_MAGIC) {
        zstd::decode_all(&bytes[..])?
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
        .ok_or_else(|| invalid_kind("document has no `kind`"))?
        .to_string();
    let event = value.get("event").and_then(|event| event.as_str());
    match (kind.as_str(), event) {
        ("pqbench.table", Some("begin")) => Ok(Record::Begin(parse_begin(value)?)),
        ("pqbench.table", Some("end")) => Ok(Record::End {
            id: string_field(&value, "id"),
        }),
        ("pqbench.table", None) => Ok(Record::Table(parse_table(value)?)),
        ("pqbench.table", Some(other)) => Err(format!("unsupported table event `{other}`").into()),
        ("pqbench.table-ref", _) => Ok(Record::TableRef(parse_table_ref(value)?)),
        ("pqbench.table-log", _) => {
            let (id, commit) = parse_log(value)?;
            Ok(Record::Log { id, commit })
        }
        ("pqbench.table-file", _) => {
            let (id, file) = parse_file(value)?;
            Ok(Record::File { id, file })
        }
        ("pqbench.remote-source", _) => Ok(Record::RemoteSource(parse_remote(value)?)),
        (other, _) => Err(invalid_kind(other)),
    }
}

fn string_field(value: &serde_json::Value, name: &str) -> String {
    value
        .get(name)
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string()
}

fn parse_begin(value: serde_json::Value) -> Result<Begin, CliError> {
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct Wire {
        #[serde(default)]
        id: String,
        version: u32,
        format: TableFormat,
        uri: String,
        snapshot_version: u64,
        #[serde(default)]
        partition_columns: Vec<String>,
        #[serde(default)]
        env: BTreeMap<String, String>,
    }
    let wire: Wire = serde_json::from_value(value).map_err(invalid_json)?;
    if wire.version != 1 {
        return Err("unsupported table document; expected kind `pqbench.table` version 1".into());
    }
    aws_env_only(&wire.env)?;
    let id = if wire.id.is_empty() {
        wire.uri.clone()
    } else {
        wire.id
    };
    Ok(Begin { id, env: wire.env })
}

fn parse_table_ref(value: serde_json::Value) -> Result<TableRef, CliError> {
    #[derive(Deserialize)]
    struct Wire {
        #[serde(default)]
        id: String,
        version: u32,
        uri: String,
        #[serde(default)]
        env: BTreeMap<String, String>,
    }
    let wire: Wire = serde_json::from_value(value).map_err(invalid_json)?;
    if wire.version != 1 {
        return Err(
            "unsupported table-ref document; expected kind `pqbench.table-ref` version 1".into(),
        );
    }
    aws_env_only(&wire.env)?;
    let id = if wire.id.is_empty() {
        wire.uri.clone()
    } else {
        wire.id
    };
    Ok(TableRef {
        id,
        uri: wire.uri,
        env: wire.env,
    })
}

fn parse_table(value: serde_json::Value) -> Result<TableInfo, CliError> {
    let table: TableInfo = serde_json::from_value(value).map_err(invalid_json)?;
    if table.version != 1 {
        return Err("unsupported table document; expected kind `pqbench.table` version 1".into());
    }
    aws_env_only(&table.env)?;
    Ok(table)
}

fn parse_log(value: serde_json::Value) -> Result<(String, LogCommit), CliError> {
    #[derive(Deserialize)]
    struct Wire {
        #[serde(default)]
        id: String,
        #[serde(flatten)]
        commit: LogCommit,
    }
    let wire = serde_json::from_value::<Wire>(value).map_err(invalid_json)?;
    Ok((wire.id, wire.commit))
}

fn parse_file(value: serde_json::Value) -> Result<(String, TableFile), CliError> {
    #[derive(Deserialize)]
    struct Wire {
        #[serde(default)]
        id: String,
        #[serde(flatten)]
        file: TableFile,
    }
    let wire = serde_json::from_value::<Wire>(value).map_err(invalid_json)?;
    Ok((wire.id, wire.file))
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
    aws_env_only(&source.env)?;
    Ok(source)
}

fn aws_env_only(env: &BTreeMap<String, String>) -> Result<(), CliError> {
    if let Some(key) = env.keys().find(|key| !key.starts_with("AWS_")) {
        return Err(format!("document may only set AWS_* variables, not `{key}`").into());
    }
    Ok(())
}

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Whether a path is `-`, JSON (`{`), or a zstd frame.
pub(crate) async fn looks_like_document(path: &str) -> bool {
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
    if n >= 4 && buf[..4] == ZSTD_MAGIC {
        return true;
    }
    buf[..n]
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'{')
}

/// Write one table's records, tagged with `id`. Safe to call for many tables
/// on the same sink; lines from different ids may mix.
pub(crate) fn write_table_records(
    emit: &mut Emit,
    id: &str,
    info: &TableInfo,
) -> Result<(), CliError> {
    emit.write(&BeginRecord {
        kind: "pqbench.table",
        version: 1,
        event: "begin",
        id,
        format: info.format,
        uri: &info.uri,
        snapshot_version: info.snapshot_version,
        partition_columns: &info.partition_columns,
        env: &info.env,
    })?;
    for commit in &info.log {
        emit.write(&KindCommit {
            kind: "pqbench.table-log",
            id,
            commit,
        })?;
    }
    for file in &info.files {
        emit.write(&KindFile {
            kind: "pqbench.table-file",
            id,
            file,
        })?;
    }
    emit.write(&EndRecord {
        kind: "pqbench.table",
        event: "end",
        id,
    })
}

#[derive(Serialize)]
struct BeginRecord<'a> {
    kind: &'static str,
    version: u32,
    event: &'static str,
    id: &'a str,
    format: TableFormat,
    uri: &'a str,
    snapshot_version: u64,
    partition_columns: &'a [String],
    #[serde(skip_serializing_if = "map_empty")]
    env: &'a BTreeMap<String, String>,
}

fn map_empty(env: &&BTreeMap<String, String>) -> bool {
    env.is_empty()
}

#[derive(Serialize)]
struct KindCommit<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(flatten)]
    commit: &'a LogCommit,
}

#[derive(Serialize)]
struct KindFile<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(flatten)]
    file: &'a TableFile,
}

#[derive(Serialize)]
struct EndRecord<'a> {
    kind: &'static str,
    event: &'static str,
    id: &'a str,
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
    format!("unsupported document kind `{kind}`; expected pqbench.table, pqbench.table-ref, or pqbench.remote-source")
        .into()
}
