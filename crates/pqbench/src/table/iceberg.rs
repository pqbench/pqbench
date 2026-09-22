//! Iceberg metadata loader.
//!
//! [`load`] reads the metadata JSON and Avro manifests and returns a
//! [`TableInfo`]. It does not measure Parquet footers; pipe the document to
//! `bytemass`. Delete files are named in the log and omitted from `files`.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Component, Path, PathBuf};

use apache_avro::{from_value, Reader};
use serde::Deserialize;
use url::Url;

use super::{LoadRequest, LogAction, LogCommit, TableFile, TableFormat, TableInfo};
use crate::object_store;

/// Errors resolving an Iceberg snapshot.
#[derive(Debug)]
pub struct Error(String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "iceberg: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// Load the current or requested Iceberg snapshot into a table document.
///
/// `request.uri` is a table root (`metadata/version-hint.text` or
/// `metadata/*.metadata.json`) or a metadata JSON path/URI. Run inside a Tokio
/// runtime.
///
/// # Errors
/// Fails for invalid metadata, missing snapshots, non-Parquet data files, or
/// data paths outside the table location.
pub async fn load(request: &LoadRequest) -> Result<TableInfo, Error> {
    let options = env_options(&request.env);
    let metadata_location = resolve_metadata_location(&request.uri, &options).await?;
    let metadata_bytes = read_location(&metadata_location, &options).await?;
    let metadata: TableMetadata = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| Error(format!("cannot parse metadata JSON: {e}")))?;
    if metadata.format_version != 1 && metadata.format_version != 2 {
        return Err(Error(format!(
            "unsupported Iceberg format version: {}",
            metadata.format_version
        )));
    }
    let requested = request.version.map(i64_from_u64).transpose()?;
    let selected = select_snapshot(&metadata, requested)?;
    let snapshot_version = selected
        .as_ref()
        .map(|snapshot| u64_from_i64(snapshot.snapshot_id))
        .transpose()?
        .unwrap_or(0);
    let (files, deletes) = match selected {
        Some(snapshot) => {
            active_files(&metadata.location, &snapshot.manifest_list, &options).await?
        }
        None => (Vec::new(), Vec::new()),
    };
    let mut log = ancestry(&metadata, selected)
        .into_iter()
        .map(|snapshot| {
            Ok(LogCommit {
                version: u64_from_i64(snapshot.snapshot_id)?,
                actions: vec![LogAction {
                    kind: "snapshot".into(),
                    path: Some(snapshot.manifest_list.clone()),
                }],
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    if let Some(commit) = log
        .iter_mut()
        .find(|commit| commit.version == snapshot_version)
    {
        for file in &files {
            commit.actions.push(LogAction {
                kind: "add".into(),
                path: Some(file.path.clone()),
            });
        }
        for path in deletes {
            commit.actions.push(LogAction {
                kind: "delete".into(),
                path: Some(path),
            });
        }
    }
    Ok(TableInfo {
        kind: "pqbench.table".into(),
        version: 1,
        format: TableFormat::ICEBERG,
        uri: request.uri.clone(),
        snapshot_version,
        partition_columns: partition_columns(&metadata),
        log,
        files,
        env: request.env.clone(),
    })
}

#[derive(Debug, Deserialize)]
struct TableMetadata {
    #[serde(rename = "format-version")]
    format_version: i32,
    location: String,
    #[serde(rename = "current-snapshot-id", default)]
    current_snapshot_id: Option<i64>,
    #[serde(rename = "default-spec-id", default)]
    default_spec_id: i32,
    #[serde(rename = "partition-specs", default)]
    partition_specs: Vec<PartitionSpec>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
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
    file_size_in_bytes: i64,
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
    let bytes = read_location(&hint, options).await?;
    let version = parse_hint_version(&String::from_utf8_lossy(&bytes))?;
    for name in hint_names(version) {
        let candidate = join_uri(uri, &format!("metadata/{name}"))?;
        if object_exists(&candidate, options).await? {
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
    let hint_path = metadata.join("version-hint.text");
    if hint_path.is_file() {
        let hint = std::fs::read_to_string(&hint_path)
            .map_err(|e| Error(format!("cannot read {}: {e}", hint_path.display())))?;
        let version = parse_hint_version(&hint)?;
        for name in hint_names(version) {
            let candidate = metadata.join(name);
            if candidate.is_file() {
                return Ok(candidate.to_string_lossy().into_owned());
            }
        }
        return Err(Error(format!(
            "version-hint.text is {version} but no matching metadata JSON is in {}",
            metadata.display()
        )));
    }
    let entries = std::fs::read_dir(&metadata)
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
    stem.parse().ok()
}

async fn active_files(
    table_location: &str,
    manifest_list: &str,
    options: &[(String, String)],
) -> Result<(Vec<TableFile>, Vec<String>), Error> {
    let root = table_root(table_location)?;
    let bytes = read_location(manifest_list, options).await?;
    let manifests: Vec<ManifestFile> = read_avro(&bytes, "manifest list")?;
    let mut files = Vec::new();
    let mut deletes = Vec::new();
    for manifest in manifests {
        let bytes = read_location(&manifest.manifest_path, options).await?;
        let entries: Vec<ManifestEntry> = read_avro(&bytes, "manifest")?;
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
            if !entry.data_file.file_format.eq_ignore_ascii_case("parquet") {
                return Err(Error(format!(
                    "active data file is not Parquet: {}",
                    entry.data_file.file_path
                )));
            }
            let size = u64::try_from(entry.data_file.file_size_in_bytes).map_err(|_| {
                Error(format!(
                    "invalid file size in manifest: {}",
                    entry.data_file.file_path
                ))
            })?;
            let uri = resolve_data_file(&root, &entry.data_file.file_path)?;
            files.push(TableFile {
                path: entry.data_file.file_path,
                uri,
                size,
            });
        }
    }
    Ok((files, deletes))
}

fn read_avro<T: for<'de> Deserialize<'de>>(bytes: &[u8], kind: &str) -> Result<Vec<T>, Error> {
    let reader = Reader::new(Cursor::new(bytes))
        .map_err(|e| Error(format!("cannot read Iceberg {kind}: {e}")))?;
    reader
        .map(|value| {
            let value = value.map_err(|e| Error(format!("cannot read Iceberg {kind}: {e}")))?;
            from_value::<T>(&value).map_err(|e| Error(format!("cannot parse Iceberg {kind}: {e}")))
        })
        .collect()
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

async fn read_location(location: &str, options: &[(String, String)]) -> Result<Vec<u8>, Error> {
    if looks_like_uri(location) {
        let reader = object_store::open(location, options).map_err(|e| Error(e.to_string()))?;
        let stat = reader.stat().await.map_err(|e| Error(e.to_string()))?;
        reader
            .read_range(0..stat.size, stat.identity.as_deref())
            .await
            .map_err(|e| Error(e.to_string()))
    } else {
        std::fs::read(location).map_err(|e| Error(format!("cannot read {location}: {e}")))
    }
}

async fn object_exists(location: &str, options: &[(String, String)]) -> Result<bool, Error> {
    let reader = object_store::open(location, options).map_err(|e| Error(e.to_string()))?;
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

fn u64_from_i64(value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| Error(format!("Iceberg snapshot id is negative: {value}")))
}

fn i64_from_u64(value: u64) -> Result<i64, Error> {
    i64::try_from(value).map_err(|_| Error(format!("Iceberg snapshot id is too large: {value}")))
}
