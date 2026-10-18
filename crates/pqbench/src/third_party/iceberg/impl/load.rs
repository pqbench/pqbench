//! Iceberg metadata loader: the only module that reads Iceberg metadata.
//!
//! [`load`] reads the metadata JSON and Avro manifests and returns a
//! [`TableInfo`]. It does not measure Parquet footers; pipe the document to
//! `bytemass`. Delete files are named in the log and omitted from `files`.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use futures_util::Stream;
use serde::Deserialize;
use serde_json::Value;
use url::Url;

use crate::table::{Column, LoadRequest, LogAction, LogCommit, TableFile, TableFormat, TableInfo};
use crate::third_party::avro::read_avro;
use crate::third_party::object_store;
use crate::Lake;

/// Errors resolving an Iceberg snapshot.
#[derive(Debug)]
pub(super) struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "iceberg: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// Load the current or requested Iceberg snapshot into a table record.
///
/// `request.uri` is a table root (`metadata/version-hint.text` or
/// `metadata/*.metadata.json`) or a metadata JSON path/URI. Run inside a Tokio
/// runtime.
///
/// # Errors
/// Fails for invalid metadata, missing snapshots, non-Parquet data files, or
/// data paths outside the table location.
pub(super) async fn load(lake: &Lake, request: &LoadRequest) -> Result<TableInfo, Error> {
    let options = env_options(&request.env);
    let metadata = read_metadata(lake, &request.uri, &options).await?;
    let requested = request.snapshot_version.map(create_i64).transpose()?;
    let selected = select_snapshot(&metadata, requested)?;
    let snapshot_version = selected
        .as_ref()
        .map(|snapshot| create_u64(snapshot.snapshot_id))
        .transpose()?
        .unwrap_or(0);
    let (files, deletes) = match selected {
        Some(snapshot) if request.require_files => {
            active_files(lake, &metadata.location, &snapshot.manifest_list, &options).await?
        }
        _ => (Vec::new(), Vec::new()),
    };
    let mut log = if request.require_files {
        ancestry_log(&metadata, selected)?
    } else {
        Vec::new()
    };
    attach_actions(&mut log, snapshot_version, &files, &deletes);
    let mut info = TableInfo::new(
        TableFormat::ICEBERG,
        request.uri.clone(),
        snapshot_version,
        partition_columns(&metadata),
        log,
        files,
        request.env.clone(),
    );
    info.columns = columns(&metadata);
    info.iceberg_properties = metadata.properties.clone();
    Ok(info)
}

/// Resolve, read, parse, and validate the table's metadata JSON.
async fn read_metadata(
    lake: &Lake,
    uri: &str,
    options: &[(String, String)],
) -> Result<TableMetadata, Error> {
    let metadata_location = resolve_metadata_location(lake, uri, options).await?;
    let metadata_bytes = read_location(lake, &metadata_location, options).await?;
    let metadata: TableMetadata = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| Error(format!("cannot parse metadata JSON: {e}")))?;
    if metadata.format_version != 1 && metadata.format_version != 2 {
        return Err(Error(format!(
            "unsupported Iceberg format version: {}",
            metadata.format_version
        )));
    }
    Ok(metadata)
}

/// The selected snapshot's ancestry as log commits, one action per snapshot.
fn ancestry_log(
    metadata: &TableMetadata,
    selected: Option<&Snapshot>,
) -> Result<Vec<LogCommit>, Error> {
    ancestry(metadata, selected)
        .into_iter()
        .map(|snapshot| {
            Ok(LogCommit {
                version: create_u64(snapshot.snapshot_id)?,
                commit_time: snapshot.snapshot_time,
                commit_info: None,
                actions: vec![LogAction {
                    kind: "snapshot".into(),
                    path: Some(snapshot.manifest_list.clone()),
                }],
            })
        })
        .collect()
}

/// Attach the snapshot's added and deleted files to its log commit.
fn attach_actions(log: &mut [LogCommit], version: u64, files: &[TableFile], deletes: &[String]) {
    let Some(commit) = log.iter_mut().find(|commit| commit.version == version) else {
        return;
    };
    for file in files {
        commit.actions.push(LogAction {
            kind: "add".into(),
            path: Some(file.path.clone()),
        });
    }
    for path in deletes {
        commit.actions.push(LogAction {
            kind: "delete".into(),
            path: Some(path.clone()),
        });
    }
}

#[derive(Debug, Deserialize)]
struct TableMetadata {
    #[serde(rename = "format-version")]
    format_version: i32,
    location: String,
    #[serde(rename = "current-snapshot-id", default)]
    current_snapshot_id: Option<i64>,
    #[serde(rename = "current-schema-id", default)]
    current_schema_id: i32,
    #[serde(rename = "default-spec-id", default)]
    default_spec_id: i32,
    #[serde(rename = "partition-specs", default)]
    partition_specs: Vec<PartitionSpec>,
    #[serde(default)]
    schemas: Vec<Schema>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
}

/// One Iceberg schema; the current one carries the table's columns.
#[derive(Debug, Deserialize)]
struct Schema {
    #[serde(rename = "schema-id")]
    schema_id: i32,
    #[serde(default)]
    fields: Vec<Field>,
}

/// One top-level Iceberg schema field.
#[derive(Debug, Deserialize)]
struct Field {
    name: String,
    #[serde(rename = "type")]
    data_type: Value,
    #[serde(default)]
    required: bool,
}

#[derive(Debug, Deserialize)]
struct PartitionSpec {
    #[serde(rename = "spec-id")]
    spec_id: i32,
    #[serde(default)]
    fields: Vec<PartitionField>,
}

#[derive(Debug, Deserialize)]
struct PartitionField {
    name: String,
}

#[derive(Debug, Deserialize)]
struct Snapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", default)]
    parent_snapshot_id: Option<i64>,
    #[serde(rename = "manifest-list")]
    manifest_list: String,
    #[serde(rename = "timestamp-ms", default)]
    snapshot_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ManifestFile {
    manifest_path: String,
    #[serde(default)]
    content: i32,
}

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    status: i32,
    data_file: DataFile,
}

#[derive(Debug, Deserialize)]
struct DataFile {
    #[serde(default)]
    content: i32,
    file_path: String,
    file_format: String,
    #[serde(rename = "file_size_in_bytes")]
    file_size_bytes: i64,
}

const STATUS_DELETED: i32 = 2;
const CONTENT_DATA: i32 = 0;
const MANIFEST_DELETES: i32 = 1;

fn partition_columns(metadata: &TableMetadata) -> Vec<String> {
    metadata
        .partition_specs
        .iter()
        .find(|spec| spec.spec_id == metadata.default_spec_id)
        .or_else(|| metadata.partition_specs.first())
        .map(|spec| spec.fields.iter().map(|field| field.name.clone()).collect())
        .unwrap_or_default()
}

/// The columns of the current schema; the first schema when none is marked.
fn columns(metadata: &TableMetadata) -> Vec<Column> {
    metadata
        .schemas
        .iter()
        .find(|schema| schema.schema_id == metadata.current_schema_id)
        .or_else(|| metadata.schemas.first())
        .map(|schema| {
            schema
                .fields
                .iter()
                .map(|field| Column {
                    name: field.name.clone(),
                    data_type: type_name(&field.data_type),
                    nullable: !field.required,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A primitive type is its name; a nested type stays the format's JSON form.
fn type_name(value: &Value) -> String {
    match value {
        Value::String(name) => name.clone(),
        other => other.to_string(),
    }
}

fn select_snapshot(
    metadata: &TableMetadata,
    requested: Option<i64>,
) -> Result<Option<&Snapshot>, Error> {
    let snapshot_id = match requested {
        Some(snapshot_id) => snapshot_id,
        None => match metadata.current_snapshot_id.filter(|id| *id != -1) {
            Some(snapshot_id) => snapshot_id,
            None => return Ok(None),
        },
    };
    metadata
        .snapshots
        .iter()
        .find(|snapshot| snapshot.snapshot_id == snapshot_id)
        .map(Some)
        .ok_or_else(|| Error(format!("Iceberg snapshot does not exist: {snapshot_id}")))
}

/// Visit the table's snapshots, newest first, without collecting them.
///
/// The metadata JSON carries the snapshot ancestry, so no manifest or data file
/// is read.
///
/// Iceberg's history is the selected snapshot's ancestry — a list the metadata
/// JSON already fixes — so the read resolves it and streams it in order.
pub(super) async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<impl Stream<Item = Result<LogCommit, Error>>, Error> {
    let options = env_options(&request.env);
    let metadata_location = resolve_metadata_location(lake, &request.uri, &options).await?;
    let metadata_bytes = read_location(lake, &metadata_location, &options).await?;
    let metadata: TableMetadata = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| Error(format!("cannot parse metadata JSON: {e}")))?;
    if metadata.format_version != 1 && metadata.format_version != 2 {
        return Err(Error(format!(
            "unsupported Iceberg format version: {}",
            metadata.format_version
        )));
    }
    let requested = request.snapshot_version.map(create_i64).transpose()?;
    let selected = select_snapshot(&metadata, requested)?;
    // `ancestry` walks from the selected snapshot back through its parents, so
    // the newest snapshot comes first.
    let mut commits = Vec::new();
    for snapshot in ancestry(&metadata, selected) {
        commits.push(LogCommit {
            version: create_u64(snapshot.snapshot_id)?,
            commit_time: snapshot.snapshot_time,
            commit_info: None,
            actions: Vec::new(),
        });
    }
    Ok(futures_util::stream::iter(commits.into_iter().map(Ok)))
}

/// Stream the files the selected snapshot's manifests name.
///
/// An Iceberg snapshot attaches its files only when it is resolved, and this read
/// resolves one snapshot — the requested one, or the latest — so the files are
/// that snapshot's. A request that names commits and does not name that snapshot
/// yields none: what an older snapshot added is a separate manifest read.
///
/// The size is left zero, as [`TableFile`] documents: the manifest's size is a
/// cross-check, and `bytemass` reads the true one from the footer.
pub(super) async fn stream_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<impl Stream<Item = Result<TableFile, Error>>, Error> {
    let mut request = request.clone();
    request.require_files = true;
    let info = load(lake, &request).await?;
    let root = request.uri.clone();
    let files = match request.commit_versions.as_ref() {
        Some(named) if !named.contains(&info.snapshot_version) => Vec::new(),
        _ => info.files,
    };
    Ok(futures_util::stream::iter(files.into_iter().map(
        move |file| {
            let uri = crate::table::join_uri(&root, &file.path)
                .map_err(|error| Error(error.to_string()))?;
            Ok(TableFile::new(file.path, uri, 0))
        },
    )))
}

fn ancestry<'a>(metadata: &'a TableMetadata, selected: Option<&'a Snapshot>) -> Vec<&'a Snapshot> {
    let Some(selected) = selected else {
        return Vec::new();
    };
    let mut chain = Vec::new();
    let mut current = Some(selected.snapshot_id);
    let mut seen = std::collections::BTreeSet::new();
    while let Some(id) = current {
        if !seen.insert(id) {
            break;
        }
        let Some(snapshot) = metadata
            .snapshots
            .iter()
            .find(|snapshot| snapshot.snapshot_id == id)
        else {
            break;
        };
        chain.push(snapshot);
        current = snapshot.parent_snapshot_id.filter(|parent| *parent != -1);
    }
    chain.reverse();
    chain
}

async fn resolve_metadata_location(
    lake: &Lake,
    uri: &str,
    options: &[(String, String)],
) -> Result<String, Error> {
    if is_metadata_json(uri) {
        return Ok(uri.to_string());
    }
    if is_local(uri) {
        return local_metadata(&local_path(uri)?);
    }
    let hint = join_uri(uri, "metadata/version-hint.text")?;
    let bytes = read_location(lake, &hint, options).await?;
    let version = parse_hint_version(&String::from_utf8_lossy(&bytes))?;
    for name in hint_names(version) {
        let candidate = join_uri(uri, &format!("metadata/{name}"))?;
        if object_exists(lake, &candidate, options).await? {
            return Ok(candidate);
        }
    }
    Err(Error(format!(
        "cannot resolve Iceberg metadata for version {version}; pass the metadata JSON location"
    )))
}

fn local_metadata(root: &Path) -> Result<String, Error> {
    if root.is_file() {
        return Ok(root.to_string_lossy().into_owned());
    }
    let metadata = root.join("metadata");
    match hint_metadata(&metadata)? {
        Some(found) => Ok(found),
        None => newest_metadata(&metadata),
    }
}

/// Resolve the metadata JSON named by `version-hint.text`, when the directory
/// has one.
fn hint_metadata(metadata: &Path) -> Result<Option<String>, Error> {
    let hint_path = metadata.join("version-hint.text");
    if !hint_path.is_file() {
        return Ok(None);
    }
    let hint = std::fs::read_to_string(&hint_path)
        .map_err(|e| Error(format!("cannot read {}: {e}", hint_path.display())))?;
    let version = parse_hint_version(&hint)?;
    for name in hint_names(version) {
        let candidate = metadata.join(name);
        if candidate.is_file() {
            return Ok(Some(candidate.to_string_lossy().into_owned()));
        }
    }
    Err(Error(format!(
        "version-hint.text is {version} but no matching metadata JSON is in {}",
        metadata.display()
    )))
}

/// The newest metadata JSON in the directory, by leading version.
fn newest_metadata(metadata: &Path) -> Result<String, Error> {
    let entries = std::fs::read_dir(metadata)
        .map_err(|e| Error(format!("cannot read {}: {e}", metadata.display())))?;
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| Error(format!("cannot read {}: {e}", metadata.display())))?;
        let name = entry.file_name();
        let Some(version) = metadata_json_version(&name.to_string_lossy()) else {
            continue;
        };
        found.push((version, entry.path()));
    }
    found.sort_by_key(|(version, _)| *version);
    found
        .pop()
        .map(|(_, path)| path.to_string_lossy().into_owned())
        .ok_or_else(|| {
            Error(format!(
                "no Iceberg metadata JSON in {}",
                metadata.display()
            ))
        })
}

fn parse_hint_version(hint: &str) -> Result<u64, Error> {
    let hint = hint.trim();
    if hint.is_empty() || hint.contains(['/', '\\']) || hint.contains("..") {
        return Err(Error(format!("invalid version-hint.text: {hint}")));
    }
    hint.parse::<u64>()
        .map_err(|_| Error(format!("version-hint.text is not a version: {hint}")))
}

fn hint_names(version: u64) -> [String; 3] {
    [
        format!("{version}.metadata.json"),
        format!("v{version}.metadata.json"),
        format!("{version:05}.metadata.json"),
    ]
}

fn metadata_json_version(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".metadata.json")?;
    let stem = stem.strip_prefix('v').unwrap_or(stem);
    // Iceberg writes `v1.metadata.json`, `1.metadata.json`, and the
    // `00001-<uuid>.metadata.json` form; only the leading version is the sort
    // key, and a bare `<uuid>.metadata.json` has none.
    let version = stem.split('-').next()?;
    if version.is_empty() || !version.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    version.parse().ok()
}

async fn active_files(
    lake: &Lake,
    table_location: &str,
    manifest_list: &str,
    options: &[(String, String)],
) -> Result<(Vec<TableFile>, Vec<String>), Error> {
    let root = table_root(table_location)?;
    let bytes = read_location(lake, manifest_list, options).await?;
    let manifests: Vec<ManifestFile> =
        read_avro(&bytes, "manifest list").map_err(|e| Error(e.to_string()))?;
    let mut files = Vec::new();
    let mut deletes = Vec::new();
    for manifest in manifests {
        let bytes = read_location(lake, &manifest.manifest_path, options).await?;
        let entries: Vec<ManifestEntry> =
            read_avro(&bytes, "manifest").map_err(|e| Error(e.to_string()))?;
        for entry in entries {
            if entry.status == STATUS_DELETED {
                continue;
            }
            let content = if manifest.content == MANIFEST_DELETES && entry.data_file.content == 0 {
                1
            } else {
                entry.data_file.content
            };
            if content != CONTENT_DATA {
                deletes.push(entry.data_file.file_path);
                continue;
            }
            files.push(data_file(&root, entry)?);
        }
    }
    Ok((files, deletes))
}

/// One active manifest entry as a `TableFile`, checking it is Parquet.
fn data_file(root: &TableRoot, entry: ManifestEntry) -> Result<TableFile, Error> {
    if !entry.data_file.file_format.eq_ignore_ascii_case("parquet") {
        return Err(Error(format!(
            "active data file is not Parquet: {}",
            entry.data_file.file_path
        )));
    }
    let size = u64::try_from(entry.data_file.file_size_bytes).map_err(|_| {
        Error(format!(
            "invalid file size in manifest: {}",
            entry.data_file.file_path
        ))
    })?;
    let uri = resolve_data_file(root, &entry.data_file.file_path)?;
    Ok(TableFile::new(entry.data_file.file_path, uri, size))
}

enum TableRoot {
    Local(PathBuf),
    Object(Url),
}

fn table_root(location: &str) -> Result<TableRoot, Error> {
    if looks_like_uri(location) {
        let url = parse_dir_url(location)?;
        if url.scheme() == "file" {
            let path = url.to_file_path().map_err(|()| {
                Error(format!(
                    "cannot convert table location to a path: {location}"
                ))
            })?;
            return Ok(TableRoot::Local(path));
        }
        return Ok(TableRoot::Object(url));
    }
    Ok(TableRoot::Local(PathBuf::from(location)))
}

fn resolve_data_file(root: &TableRoot, location: &str) -> Result<String, Error> {
    match root {
        TableRoot::Local(root) => local_uri(root, location),
        TableRoot::Object(base) => object_input(base, location),
    }
}

fn local_uri(root: &Path, location: &str) -> Result<String, Error> {
    let path = local_data_path(root, location)?;
    Ok(path.to_string_lossy().into_owned())
}

fn local_data_path(root: &Path, location: &str) -> Result<PathBuf, Error> {
    let candidate = if looks_like_uri(location) {
        let url = Url::parse(location)
            .map_err(|e| Error(format!("active data file is not a URI: {location}: {e}")))?;
        if url.scheme() != "file" {
            return Err(Error(format!("active data file is not local: {location}")));
        }
        url.to_file_path().map_err(|()| {
            Error(format!(
                "cannot convert active data file to a path: {location}"
            ))
        })?
    } else {
        relative_data_path(location)?.to_path_buf()
    };
    let local = if candidate.is_absolute() {
        candidate
    } else {
        root.join(candidate)
    };
    if !local.starts_with(root) {
        return Err(Error(format!(
            "active data file is outside the Iceberg table location: {location}"
        )));
    }
    Ok(local)
}

fn object_input(base: &Url, location: &str) -> Result<String, Error> {
    let uri = if looks_like_uri(location) {
        location.to_string()
    } else {
        relative_data_path(location)?;
        base.join(location)
            .map_err(|e| Error(format!("invalid active file path {location}: {e}")))?
            .into()
    };
    let url = Url::parse(&uri).map_err(|e| Error(e.to_string()))?;
    if url.scheme() != base.scheme()
        || url.host_str() != base.host_str()
        || !url.path().starts_with(base.path())
    {
        return Err(Error(format!(
            "active data file is outside the Iceberg table location: {location}"
        )));
    }
    Ok(uri)
}

fn relative_data_path(relative: &str) -> Result<&Path, Error> {
    let path = Path::new(relative);
    if relative.is_empty()
        || relative.contains("://")
        || !path.components().all(|c| matches!(c, Component::Normal(_)))
    {
        return Err(Error(format!(
            "only relative data paths inside the table are supported: {relative}"
        )));
    }
    Ok(path)
}

fn parse_dir_url(location: &str) -> Result<Url, Error> {
    let mut url = Url::parse(location)
        .map_err(|e| Error(format!("invalid table location {location}: {e}")))?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn join_uri(base: &str, relative: &str) -> Result<String, Error> {
    let mut url = if looks_like_uri(base) {
        Url::parse(base).map_err(|e| Error(format!("invalid table URI: {e}")))?
    } else {
        Url::from_directory_path(base).map_err(|()| Error("invalid local table path".into()))?
    };
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url
        .join(relative)
        .map_err(|e| Error(format!("invalid table path {relative}: {e}")))?
        .into())
}

async fn read_location(
    lake: &Lake,
    location: &str,
    options: &[(String, String)],
) -> Result<Vec<u8>, Error> {
    if looks_like_uri(location) {
        let reader =
            object_store::open(lake, location, options).map_err(|e| Error(e.to_string()))?;
        let stat = reader.stat().await.map_err(|e| Error(e.to_string()))?;
        reader
            .read_range(0..stat.size_bytes, stat.identity.as_deref())
            .await
            .map_err(|e| Error(e.to_string()))
    } else {
        std::fs::read(location).map_err(|e| Error(format!("cannot read {location}: {e}")))
    }
}

async fn object_exists(
    lake: &Lake,
    location: &str,
    options: &[(String, String)],
) -> Result<bool, Error> {
    let reader = object_store::open(lake, location, options).map_err(|e| Error(e.to_string()))?;
    reader.exists().await.map_err(|e| Error(e.to_string()))
}

fn env_options(env: &BTreeMap<String, String>) -> Vec<(String, String)> {
    env.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn is_metadata_json(uri: &str) -> bool {
    uri.rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.ends_with(".metadata.json"))
}

fn is_local(uri: &str) -> bool {
    !uri.contains("://") || uri.starts_with("file://")
}

fn local_path(uri: &str) -> Result<PathBuf, Error> {
    if let Some(rest) = uri.strip_prefix("file://") {
        Url::parse(uri)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .or_else(|| Some(PathBuf::from(rest)))
            .ok_or_else(|| Error("invalid local table URI".into()))
    } else {
        Ok(PathBuf::from(uri))
    }
}

fn looks_like_uri(value: &str) -> bool {
    value.contains("://")
}

fn create_u64(value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error(format!("Iceberg snapshot id is negative: {value}")))
}

fn create_i64(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error(format!("Iceberg snapshot id is too large: {value}")))
}
