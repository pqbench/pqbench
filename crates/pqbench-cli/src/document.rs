//! Documents on stdin or a file. `kind` decides what the command does.
//!
//! A producer resolves a lake or a table and pqbench measures bytes. Known
//! kinds are `pqbench.lake`, `pqbench.lake-source`, `pqbench.table`,
//! `pqbench.table-ref`, `pqbench.remote-source`, `pqbench.bytemass`, and
//! `pqbench.bytemass-row`. A pipe writes NDJSON;
//! every record carries a table `id` so many tables can mix. A terminal prints
//! a short summary and requires `-o` (zstd NDJSON). A single `pqbench.table`
//! object is still accepted. Credentials stay on the document so a pipe can
//! carry them between processes.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::emit::Emit;

use pqbench::bytemass::MassRow;
use pqbench::lake::Lake;
use pqbench::pattern::Selection;
use pqbench::table::{LogCommit, TableFile, TableFormat, TableInfo};
use serde::{Deserialize, Serialize};

use crate::CliError;

/// Credentials for listing a catalog.
///
/// Catalog host and token live in `env` (or the process environment), the
/// same way storage options do: `DATABRICKS_HOST` / `DATABRICKS_TOKEN`, or
/// `CATALOG_ENDPOINT` / `CATALOG_TOKEN`. `GET /v1/config` with a `defaults`
/// object is Iceberg REST; a 200 without `defaults`, or HTTP 404, is Unity.
/// Unity OSS often has no token. Only `AWS_*` is copied onto listed tables.
#[derive(Deserialize)]
pub(crate) struct LakeSource {
    pub version: u32,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// List this catalog, or a catalog-name glob. A literal skips `/catalogs`.
    #[serde(default)]
    pub catalog: Option<String>,
    /// List this schema, or a schema-name glob. A literal skips `/schemas`.
    /// Requires `catalog`.
    #[serde(default)]
    pub schema: Option<String>,
}

const CATALOG_ENDPOINT_KEYS: &[&str] = &["DATABRICKS_HOST", "CATALOG_ENDPOINT"];
const CATALOG_TOKEN_KEYS: &[&str] = &["DATABRICKS_TOKEN", "CATALOG_TOKEN"];

impl LakeSource {
    /// Catalog origin from `env`, then the process environment.
    pub(crate) fn catalog_endpoint(&self) -> Result<String, CliError> {
        env_value(&self.env, CATALOG_ENDPOINT_KEYS).ok_or_else(|| {
            "lake source needs DATABRICKS_HOST or CATALOG_ENDPOINT (in env or the process environment)"
                .into()
        })
    }

    /// Bearer token from `env`, then the process environment. Empty is none.
    pub(crate) fn catalog_token(&self) -> Option<String> {
        env_value(&self.env, CATALOG_TOKEN_KEYS)
    }

    /// Storage options copied onto each listed table (`AWS_*` only).
    pub(crate) fn storage_env(&self) -> BTreeMap<String, String> {
        self.env
            .iter()
            .filter(|(key, _)| key.starts_with("AWS_"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

fn env_value(env: &BTreeMap<String, String>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = env
            .get(*key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            return Some(value.to_string());
        }
    }
    for key in keys {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn catalog_env_key(key: &str) -> bool {
    CATALOG_ENDPOINT_KEYS.contains(&key) || CATALOG_TOKEN_KEYS.contains(&key)
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
    Lake(Lake),
    LakeBegin,
    LakeEnd,
    LakeSource(LakeSource),
    RemoteSource(RemoteSource),
    BytemassBegin,
    BytemassRow {
        id: String,
        row: MassRow,
    },
    BytemassEnd,
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
pub(crate) fn visit_records(
    reader: impl Read,
    mut visit: impl FnMut(Record) -> Result<(), CliError>,
) -> Result<(), CliError> {
    let de = serde_json::Deserializer::from_reader(reader);
    let mut empty = true;
    for value in de.into_iter::<serde_json::Value>() {
        empty = false;
        visit(classify(value.map_err(invalid_json)?)?)?;
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
        ("pqbench.lake", Some("begin")) => Ok(Record::LakeBegin),
        ("pqbench.lake", Some("end")) => Ok(Record::LakeEnd),
        ("pqbench.lake", None) => Ok(Record::Lake(parse_lake(value)?)),
        ("pqbench.lake", Some(other)) => Err(format!("unsupported lake event `{other}`").into()),
        ("pqbench.lake-source", _) => Ok(Record::LakeSource(parse_lake_source(value)?)),
        ("pqbench.remote-source", _) => Ok(Record::RemoteSource(parse_remote(value)?)),
        ("pqbench.bytemass", Some("begin")) => Ok(Record::BytemassBegin),
        ("pqbench.bytemass", Some("end")) => Ok(Record::BytemassEnd),
        ("pqbench.bytemass", Some(other)) => {
            Err(format!("unsupported bytemass event `{other}`").into())
        }
        ("pqbench.bytemass", None) => Err("a bytemass stream needs begin/end events".into()),
        ("pqbench.bytemass-row", _) => {
            let (id, row) = parse_mass_row(value)?;
            Ok(Record::BytemassRow { id, row })
        }
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

fn parse_lake(value: serde_json::Value) -> Result<Lake, CliError> {
    let lake: Lake = serde_json::from_value(value).map_err(invalid_json)?;
    if lake.version != 1 {
        return Err("unsupported lake document; expected kind `pqbench.lake` version 1".into());
    }
    if lake.tables.is_empty() {
        return Err("lake document contains no tables".into());
    }
    for table in &lake.tables {
        aws_env_only(&table.env)?;
        if let Some(info) = &table.info {
            if info.kind != "pqbench.table" || info.version != 1 {
                return Err("lake table info must be kind `pqbench.table` version 1".into());
            }
            aws_env_only(&info.env)?;
        }
    }
    Ok(lake)
}

fn parse_lake_source(value: serde_json::Value) -> Result<LakeSource, CliError> {
    let source: LakeSource = serde_json::from_value(value).map_err(invalid_json)?;
    if source.version != 1 {
        return Err(
            "unsupported lake source; expected kind `pqbench.lake-source` version 1".into(),
        );
    }
    if source
        .schema
        .as_deref()
        .is_some_and(|name| !name.is_empty())
        && source.catalog.as_deref().is_none_or(|name| name.is_empty())
    {
        return Err("lake source schema needs a catalog".into());
    }
    if let Some(key) = source
        .env
        .keys()
        .find(|key| !key.starts_with("AWS_") && !catalog_env_key(key))
    {
        return Err(format!(
            "lake source env may only contain AWS_* or catalog names, not `{key}`"
        )
        .into());
    }
    Ok(source)
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
pub(crate) fn looks_like_document(path: &str) -> bool {
    if path == "-" {
        return true;
    }
    let path = Path::new(path);
    if !path.is_file() {
        return false;
    }
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut buf = [0u8; 64];
    let Ok(n) = file.read(&mut buf) else {
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

/// Open a JSON or zstd document file.
pub(crate) fn open_file(path: &Path) -> Result<Box<dyn Read>, CliError> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    let n = file.read(&mut magic)?;
    let rest = std::io::Cursor::new(magic[..n].to_vec()).chain(file);
    if n == 4 && magic == ZSTD_MAGIC {
        return Ok(Box::new(zstd::Decoder::new(rest)?));
    }
    Ok(Box::new(rest))
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
        snapshot_time: info.snapshot_time.as_deref(),
        selection: &info.selection,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_time: Option<&'a str>,
    #[serde(skip_serializing_if = "selection_default")]
    selection: &'a Selection,
    partition_columns: &'a [String],
    #[serde(skip_serializing_if = "map_empty")]
    env: &'a BTreeMap<String, String>,
}

fn map_empty(env: &&BTreeMap<String, String>) -> bool {
    env.is_empty()
}

fn selection_default(selection: &&Selection) -> bool {
    selection.is_default()
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
    format!("unsupported document kind `{kind}`; expected pqbench.lake, pqbench.lake-source, pqbench.table, pqbench.table-ref, pqbench.remote-source, or pqbench.bytemass")
        .into()
}
