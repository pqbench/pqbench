//! Iceberg metadata loader: the only module that reads Iceberg metadata.
//!
//! [`load`] reads the metadata JSON and Avro manifests and returns a
//! [`TableInfo`]. Nothing here is behind a `#[cfg]`: the Avro reader and the
//! object store enforce the `iceberg` and `aws` features at runtime.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Duration, NaiveDate, SecondsFormat, Utc};
use futures_util::stream::{self, BoxStream, Stream};
use futures_util::{StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use url::Url;

use crate::table::{is_local, join_uri, local_path, relative_data_path};
use crate::third_party::avro::{read_avro_values, Value as AvroValue};
use crate::third_party::object_store;
use crate::v1;
use crate::Lake;

use super::{Error, LoadRequest, SnapshotFile, SnapshotStream, TableInfo};

const STATUS_DELETED: i32 = 2;
const CONTENT_DATA: i32 = 0;
const MANIFEST_DELETES: i32 = 1;
const VERSION_LAST: i32 = 3;

/// The metadata JSON, shared with Iceberg REST `loadTable`.
#[derive(Debug, Deserialize)]
pub(super) struct TableMetadata {
    #[serde(rename = "format-version", default)]
    format_version: i32,
    #[serde(default)]
    location: String,
    #[serde(rename = "current-snapshot-id", default)]
    pub(super) current_snapshot_id: Option<i64>,
    #[serde(rename = "current-schema-id", default)]
    current_schema_id: i32,
    #[serde(rename = "default-spec-id", default)]
    default_spec_id: i32,
    #[serde(rename = "partition-specs", default)]
    partition_specs: Vec<PartitionSpec>,
    #[serde(default)]
    schemas: Vec<Schema>,
    #[serde(default)]
    pub(super) properties: BTreeMap<String, String>,
    #[serde(default)]
    snapshots: Vec<Snapshot>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Schema {
    #[serde(rename = "schema-id", default)]
    schema_id: i32,
    #[serde(default)]
    fields: Vec<Field>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Field {
    #[serde(default)]
    id: i32,
    name: String,
    #[serde(rename = "type")]
    data_type: JsonValue,
    #[serde(default)]
    required: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct PartitionSpec {
    #[serde(rename = "spec-id", default)]
    spec_id: i32,
    #[serde(default)]
    fields: Vec<PartitionField>,
}

#[derive(Debug, Deserialize)]
pub(super) struct PartitionField {
    #[serde(rename = "source-id", default)]
    source_id: i32,
    name: String,
    // aipnaming: allow(aip-140/verbs)
    #[serde(default)]
    transform: String,
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

#[derive(Debug, Clone)]
struct ManifestFile {
    manifest_path: String,
    content: i32,
    partition_spec_id: i32,
    added_snapshot_id: Option<i64>,
}

struct EntryRecord {
    status: i32,
    snapshot_id: Option<i64>,
    content: i32,
    file_path: String,
    file_format: String,
    size_bytes: i64,
    record_count: i64,
    partition: Vec<(String, AvroValue)>,
}

pub(super) async fn load(lake: &Lake, request: &LoadRequest) -> Result<TableInfo, Error> {
    let metadata = read_metadata(lake, &request.uri).await?;
    let selected = select_snapshot(&metadata, request.snapshot_version)?;
    let snapshot_version = selected
        .map(|snapshot| create_u64(snapshot.snapshot_id))
        .transpose()?
        .unwrap_or(0);
    let (files, deletes) = match selected.filter(|_| request.require_files) {
        Some(snapshot) => active_files(lake, &metadata, snapshot).await?,
        None => (Vec::new(), Vec::new()),
    };
    let mut log = if request.require_files {
        ancestry_log(&metadata, selected, &v1::name::local_table(&request.uri))?
    } else {
        Vec::new()
    };
    if let Some(commit) = log.iter_mut().find(|c| c.version == snapshot_version) {
        commit
            .actions
            .extend(files.iter().map(|file| v1::LogAction {
                kind: Some("add".into()),
                path: Some(file.path.clone()),
                size_bytes: file.size_bytes,
            }));
        commit
            .actions
            .extend(deletes.into_iter().map(|path| v1::LogAction {
                kind: Some("delete".into()),
                path: Some(path),
                size_bytes: None,
            }));
    }
    let mut info = TableInfo::new(
        v1::TableFormat::Iceberg,
        request.uri.clone(),
        snapshot_version,
        partition_columns(&metadata),
        log,
        files,
    );
    info.columns = columns(&metadata);
    info.iceberg_properties = metadata.properties;
    Ok(info)
}

pub(super) async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<impl Stream<Item = Result<v1::Commit, Error>>, Error> {
    let metadata = read_metadata(lake, &request.uri).await?;
    let selected = select_snapshot(&metadata, request.snapshot_version)?;
    let table = v1::name::local_table(&request.uri);
    let commits = ancestry_newest_first(&metadata, selected)
        .map(|snapshot| {
            let version = create_u64(snapshot.snapshot_id)?;
            Ok(v1::Commit {
                name: v1::name::commit_name(&table, &version.to_string()),
                version,
                commit_time: snapshot.snapshot_time,
                commit_info: None,
                actions: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(stream::iter(commits.into_iter().map(Ok)))
}

pub(super) async fn stream_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<impl Stream<Item = Result<v1::TableFile, Error>>, Error> {
    let mut request = request.clone();
    request.require_files = true;
    let info = load(lake, &request).await?;
    let files = match request.commit_versions.as_ref() {
        Some(named) if !named.contains(&info.snapshot_version) => Vec::new(),
        _ => info.files,
    };
    Ok(stream::iter(files.into_iter().map(Ok)))
}

pub(super) async fn stream_active_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<super::FileStream<'static>, Error> {
    let metadata = Arc::new(read_metadata(lake, &request.uri).await?);
    let Some(snapshot) = select_snapshot(&metadata, request.snapshot_version)? else {
        return Ok(stream::empty().boxed());
    };
    let root = table_root(&metadata.location)?;
    let snapshot_id = snapshot.snapshot_id;
    let work = manifests(lake, &snapshot.manifest_list)
        .await?
        .into_iter()
        .filter(|manifest| manifest.content == CONTENT_DATA)
        .map(move |manifest| (snapshot_id, manifest));
    Ok(stream_manifests(
        lake,
        work,
        move |fallback_id, manifest, entries| {
            entries
                .into_iter()
                .filter(|entry| {
                    entry.status != STATUS_DELETED && entry_content(manifest, entry) == CONTENT_DATA
                })
                .map(|entry| data_file(&metadata, manifest, Some(fallback_id), &root, &entry))
                .collect()
        },
    ))
}

pub(super) async fn stream_removed_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<super::FileStream<'static>, Error> {
    Ok(stream_snapshot_files(lake, request)
        .await?
        .try_filter_map(|item| async move {
            Ok(match item {
                SnapshotFile::Removed(file) => Some(file),
                SnapshotFile::Active(_) | SnapshotFile::Delete(_) => None,
            })
        })
        .boxed())
}

/// Stream a snapshot's removed, active, and delete files while reading the
/// metadata JSON, each manifest list, and each manifest at most once.
pub(super) async fn stream_snapshot_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<SnapshotStream<'static>, Error> {
    let metadata = Arc::new(read_metadata(lake, &request.uri).await?);
    let Some(snapshot) = select_snapshot(&metadata, request.snapshot_version)? else {
        return Ok(stream::empty().boxed());
    };
    let snapshot_id = snapshot.snapshot_id;
    let root = table_root(&metadata.location)?;
    let current = manifests(lake, &snapshot.manifest_list).await?;
    let mut read_lists = BTreeSet::from([snapshot.manifest_list.clone()]);
    let mut seen: BTreeSet<String> = current.iter().map(|m| m.manifest_path.clone()).collect();
    let mut work = Vec::new();
    for other in &metadata.snapshots {
        if other.snapshot_id != snapshot_id && read_lists.insert(other.manifest_list.clone()) {
            work.extend(
                manifests(lake, &other.manifest_list)
                    .await?
                    .into_iter()
                    .filter(|m| m.content == CONTENT_DATA && seen.insert(m.manifest_path.clone()))
                    .map(|manifest| ((other.snapshot_id, false), manifest)),
            );
        }
    }
    work.extend(
        current
            .into_iter()
            .rev()
            .map(|manifest| ((snapshot_id, true), manifest)),
    );
    Ok(stream_manifests(
        lake,
        work.into_iter(),
        move |(fallback_id, is_current), manifest, entries| {
            let (removed, rest): (Vec<_>, Vec<_>) = entries
                .into_iter()
                .filter_map(|entry| {
                    classify_entry(&metadata, manifest, fallback_id, is_current, &root, &entry)
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .partition(|item| matches!(item, SnapshotFile::Removed(_)));
            Ok(removed.into_iter().chain(rest).collect())
        },
    ))
}

fn classify_entry(
    metadata: &TableMetadata,
    manifest: &ManifestFile,
    fallback_id: i64,
    is_current: bool,
    root: &TableRoot,
    entry: &EntryRecord,
) -> Result<Option<SnapshotFile>, Error> {
    let is_data = entry_content(manifest, entry) == CONTENT_DATA;
    match (is_current, entry.status == STATUS_DELETED, is_data) {
        (false, _, true) | (true, true, true) => {
            table_file(metadata, manifest, Some(fallback_id), root, entry)
                .map(|file| Some(SnapshotFile::Removed(file)))
        }
        (true, false, true) => data_file(metadata, manifest, Some(fallback_id), root, entry)
            .map(|file| Some(SnapshotFile::Active(file))),
        (true, false, false) => table_file(metadata, manifest, Some(fallback_id), root, entry)
            .map(|file| Some(SnapshotFile::Delete(file))),
        _ => Ok(None),
    }
}

/// The partition columns of the table: the default spec's non-`void` fields
/// first, then any non-`void` field another spec in `partition-specs` declares.
pub(super) fn partition_columns(metadata: &TableMetadata) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let (default, rest): (Vec<_>, Vec<_>) = metadata
        .partition_specs
        .iter()
        .partition(|spec| spec.spec_id == metadata.default_spec_id);
    default
        .into_iter()
        .chain(rest)
        .flat_map(|spec| &spec.fields)
        .filter(|field| field.transform != "void" && seen.insert(field.name.as_str()))
        .map(|field| field.name.clone())
        .collect()
}

/// The columns of the current schema; the first schema when none is marked.
pub(super) fn columns(metadata: &TableMetadata) -> Vec<v1::DeclaredColumn> {
    current_schema(metadata)
        .map(|schema| {
            schema
                .fields
                .iter()
                .map(|field| v1::DeclaredColumn {
                    column: field.name.clone(),
                    data_type: super::non_empty(&type_name(&field.data_type)),
                    nullable: !field.required,
                    position: None,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn current_schema(metadata: &TableMetadata) -> Option<&Schema> {
    metadata
        .schemas
        .iter()
        .find(|schema| schema.schema_id == metadata.current_schema_id)
        .or_else(|| metadata.schemas.first())
}

fn source_type(metadata: &TableMetadata, source_id: i32) -> Option<String> {
    current_schema(metadata)
        .into_iter()
        .chain(&metadata.schemas)
        .find_map(|schema| field_type(&schema.fields, source_id))
}

fn field_type(fields: &[Field], id: i32) -> Option<String> {
    fields
        .iter()
        .find(|field| field.id == id)
        .map(|field| type_name(&field.data_type))
        .or_else(|| {
            fields
                .iter()
                .find_map(|field| nested_field_type(&field.data_type, id))
        })
}

fn nested_field_type(value: &JsonValue, id: i32) -> Option<String> {
    value.get("fields")?.as_array()?.iter().find_map(|item| {
        (item.get("id")?.as_i64() == Some(i64::from(id)))
            .then(|| item.get("type").map(type_name))
            .flatten()
            .or_else(|| item.get("type").and_then(|ty| nested_field_type(ty, id)))
    })
}

fn type_name(value: &JsonValue) -> String {
    match value {
        JsonValue::String(name) => name.clone(),
        other => other.to_string(),
    }
}

async fn read_metadata(lake: &Lake, uri: &str) -> Result<TableMetadata, Error> {
    let location = resolve_metadata_location(lake, uri).await?;
    let bytes = read_location(lake, &location).await?;
    let metadata: TableMetadata = serde_json::from_slice(&bytes).map_err(Error::Json)?;
    if !(1..=VERSION_LAST).contains(&metadata.format_version) {
        return Err(Error::Invalid(format!(
            "unsupported Iceberg format version: {}",
            metadata.format_version
        )));
    }
    Ok(metadata)
}

fn select_snapshot(
    metadata: &TableMetadata,
    requested: Option<u64>,
) -> Result<Option<&Snapshot>, Error> {
    let snapshot_id = match requested {
        Some(version) => i64::try_from(version)
            .map_err(|_| Error::Invalid(format!("Iceberg snapshot id is too large: {version}")))?,
        None => match metadata.current_snapshot_id.filter(|id| *id != -1) {
            Some(id) => id,
            None => return Ok(None),
        },
    };
    metadata
        .snapshots
        .iter()
        .find(|snapshot| snapshot.snapshot_id == snapshot_id)
        .map(Some)
        .ok_or_else(|| Error::Invalid(format!("Iceberg snapshot does not exist: {snapshot_id}")))
}

fn ancestry_newest_first<'a>(
    metadata: &'a TableMetadata,
    selected: Option<&'a Snapshot>,
) -> impl Iterator<Item = &'a Snapshot> {
    let mut seen = BTreeSet::new();
    std::iter::successors(selected, move |snapshot| {
        snapshot
            .parent_snapshot_id
            .filter(|parent| *parent != -1)
            .and_then(|id| metadata.snapshots.iter().find(|s| s.snapshot_id == id))
    })
    .take_while(move |snapshot| seen.insert(snapshot.snapshot_id))
}

fn ancestry_log(
    metadata: &TableMetadata,
    selected: Option<&Snapshot>,
    table: &str,
) -> Result<Vec<v1::Commit>, Error> {
    ancestry_newest_first(metadata, selected)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|snapshot| {
            let version = create_u64(snapshot.snapshot_id)?;
            Ok(v1::Commit {
                name: v1::name::commit_name(table, &version.to_string()),
                version,
                commit_time: snapshot.snapshot_time,
                commit_info: None,
                actions: vec![v1::LogAction {
                    kind: Some("snapshot".into()),
                    path: Some(snapshot.manifest_list.clone()),
                    size_bytes: None,
                }],
            })
        })
        .collect()
}

async fn active_files(
    lake: &Lake,
    metadata: &TableMetadata,
    snapshot: &Snapshot,
) -> Result<(Vec<v1::TableFile>, Vec<String>), Error> {
    let root = table_root(&metadata.location)?;
    let (mut files, mut deletes) = (Vec::new(), Vec::new());
    for manifest in manifests(lake, &snapshot.manifest_list).await? {
        for entry in manifest_entries(lake, &manifest).await? {
            if entry.status == STATUS_DELETED {
                continue;
            }
            if entry_content(&manifest, &entry) == CONTENT_DATA {
                files.push(data_file(
                    metadata,
                    &manifest,
                    Some(snapshot.snapshot_id),
                    &root,
                    &entry,
                )?);
            } else {
                deletes.push(
                    data_file_parts(&root, &entry.file_path)
                        .map_or(entry.file_path, |(_, relative)| relative),
                );
            }
        }
    }
    Ok((files, deletes))
}

fn stream_manifests<I, T, F>(
    lake: &Lake,
    work: impl Iterator<Item = (I, ManifestFile)> + Send + 'static,
    map_manifest: F,
) -> BoxStream<'static, Result<T, Error>>
where
    I: Send + 'static,
    T: Send + 'static,
    F: Fn(I, &ManifestFile, Vec<EntryRecord>) -> Result<Vec<T>, Error> + Send + Sync + 'static,
{
    let lake = lake.clone();
    let map_manifest = Arc::new(map_manifest);
    stream::iter(work)
        .then(move |(context, manifest)| {
            let lake = lake.clone();
            let map_manifest = Arc::clone(&map_manifest);
            async move {
                let entries = manifest_entries(&lake, &manifest).await?;
                map_manifest(context, &manifest, entries)
            }
        })
        .map_ok(|items| stream::iter(items.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
}

async fn resolve_metadata_location(lake: &Lake, uri: &str) -> Result<String, Error> {
    if uri
        .rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.ends_with(".metadata.json"))
    {
        return Ok(uri.to_string());
    }
    if is_local(uri) {
        return local_metadata(&local_path(uri)?);
    }
    let hint = join_uri(uri, "metadata/version-hint.text")?;
    let version = parse_hint_version(&String::from_utf8_lossy(&read_location(lake, &hint).await?))?;
    for name in hint_names(version) {
        let candidate = join_uri(uri, &format!("metadata/{name}"))?;
        let reader =
            object_store::open(lake, &candidate).map_err(|e| Error::Storage(Box::new(e)))?;
        if reader
            .exists()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?
        {
            return Ok(candidate);
        }
    }
    Err(Error::Invalid(format!(
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
        let hint =
            std::fs::read_to_string(&hint_path).map_err(|source| io_error(&hint_path, source))?;
        let version = parse_hint_version(&hint)?;
        return hint_names(version)
            .into_iter()
            .map(|name| metadata.join(name))
            .find(|candidate| candidate.is_file())
            .map(|path| path.to_string_lossy().into_owned())
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "version-hint.text is {version} but no matching metadata JSON is in {}",
                    metadata.display()
                ))
            });
    }
    std::fs::read_dir(&metadata)
        .map_err(|source| io_error(&metadata, source))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| io_error(&metadata, source))?
        .into_iter()
        .filter_map(|entry| {
            metadata_json_version(&entry.file_name().to_string_lossy())
                .map(|version| (version, entry.path()))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, path)| path.to_string_lossy().into_owned())
        .ok_or_else(|| {
            Error::Invalid(format!(
                "no Iceberg metadata JSON in {}",
                metadata.display()
            ))
        })
}

fn parse_hint_version(hint: &str) -> Result<u64, Error> {
    let hint = hint.trim();
    if hint.is_empty() || hint.contains(['/', '\\']) || hint.contains("..") {
        return Err(Error::Invalid(format!("invalid version-hint.text: {hint}")));
    }
    hint.parse::<u64>()
        .map_err(|_| Error::Invalid(format!("version-hint.text is not a version: {hint}")))
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
    let version = stem.strip_prefix('v').unwrap_or(stem).split('-').next()?;
    (!version.is_empty() && version.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| version.parse().ok())
        .flatten()
}

fn data_file(
    metadata: &TableMetadata,
    manifest: &ManifestFile,
    fallback_snapshot_id: Option<i64>,
    root: &TableRoot,
    entry: &EntryRecord,
) -> Result<v1::TableFile, Error> {
    if !entry.file_format.eq_ignore_ascii_case("parquet") {
        return Err(Error::Invalid(format!(
            "active data file is not Parquet: {}",
            entry.file_path
        )));
    }
    table_file(metadata, manifest, fallback_snapshot_id, root, entry)
}

fn table_file(
    metadata: &TableMetadata,
    manifest: &ManifestFile,
    fallback_snapshot_id: Option<i64>,
    root: &TableRoot,
    entry: &EntryRecord,
) -> Result<v1::TableFile, Error> {
    let size = u64::try_from(entry.size_bytes).map_err(|_| {
        Error::Invalid(format!(
            "invalid file size in manifest: {}",
            entry.file_path
        ))
    })?;
    let (uri, relative) = data_file_parts(root, &entry.file_path)?;
    let snapshot_id = entry
        .snapshot_id
        .or(manifest.added_snapshot_id)
        .or(fallback_snapshot_id);
    let modification_time = snapshot_id
        .and_then(|id| metadata.snapshots.iter().find(|s| s.snapshot_id == id))
        .and_then(|snapshot| snapshot.snapshot_time)
        .and_then(|time| u64::try_from(time).ok());
    Ok(v1::TableFile {
        blob: Some(v1::name::blob_name(&uri)),
        path: relative,
        size_bytes: Some(size),
        modification_time,
        partition_values: partition_values(metadata, manifest, &entry.partition),
        stats: u64::try_from(entry.record_count)
            .ok()
            .map(|record_count| v1::FileStats {
                record_count,
                ..v1::FileStats::default()
            }),
        ..v1::TableFile::default()
    })
}

fn entry_content(manifest: &ManifestFile, entry: &EntryRecord) -> i32 {
    if manifest.content == MANIFEST_DELETES && entry.content == CONTENT_DATA {
        MANIFEST_DELETES
    } else {
        entry.content
    }
}

async fn manifests(lake: &Lake, manifest_list: &str) -> Result<Vec<ManifestFile>, Error> {
    read_avro_values(&read_location(lake, manifest_list).await?, "manifest list")
        .map_err(Error::Avro)?
        .into_iter()
        .map(|row| {
            Ok(ManifestFile {
                manifest_path: row
                    .str("manifest_path")
                    .ok_or_else(|| {
                        Error::Invalid(
                            "an Iceberg manifest-list entry names no manifest_path".into(),
                        )
                    })?
                    .to_owned(),
                content: row.int("content").unwrap_or(0) as i32,
                partition_spec_id: row.int("partition_spec_id").unwrap_or(0) as i32,
                added_snapshot_id: row.int("added_snapshot_id"),
            })
        })
        .collect()
}

async fn manifest_entries(lake: &Lake, manifest: &ManifestFile) -> Result<Vec<EntryRecord>, Error> {
    read_avro_values(
        &read_location(lake, &manifest.manifest_path).await?,
        "manifest",
    )
    .map_err(Error::Avro)?
    .into_iter()
    .map(|row| {
        let data_file = row
            .get("data_file")
            .filter(|value| value.fields().is_some())
            .ok_or_else(|| Error::Invalid("an Iceberg manifest entry names no data_file".into()))?;
        Ok(EntryRecord {
            status: row.int("status").unwrap_or(0) as i32,
            snapshot_id: row.int("snapshot_id"),
            content: data_file.int("content").unwrap_or(0) as i32,
            file_path: data_file.str("file_path").unwrap_or_default().to_owned(),
            file_format: data_file.str("file_format").unwrap_or_default().to_owned(),
            size_bytes: data_file.int("file_size_in_bytes").unwrap_or(0),
            record_count: data_file.int("record_count").unwrap_or(-1),
            partition: data_file
                .get("partition")
                .and_then(AvroValue::fields)
                .unwrap_or_default()
                .to_vec(),
        })
    })
    .collect()
}

fn partition_values(
    metadata: &TableMetadata,
    manifest: &ManifestFile,
    partition: &[(String, AvroValue)],
) -> BTreeMap<String, Option<String>> {
    let spec = metadata
        .partition_specs
        .iter()
        .find(|spec| spec.spec_id == manifest.partition_spec_id)
        .or_else(|| {
            metadata
                .partition_specs
                .iter()
                .find(|spec| spec.spec_id == metadata.default_spec_id)
        })
        .or_else(|| metadata.partition_specs.first());
    partition
        .iter()
        .filter_map(|(name, value)| {
            let field = spec.and_then(|spec| spec.fields.iter().find(|f| &f.name == name));
            let transform = field.map_or("identity", |f| f.transform.as_str());
            if transform == "void" && matches!(value, AvroValue::Null) {
                return None;
            }
            let source = field.and_then(|f| source_type(metadata, f.source_id));
            let text = (!matches!(value, AvroValue::Null))
                .then(|| partition_text(transform, source.as_deref(), value));
            Some((name.clone(), text))
        })
        .collect()
}

fn partition_text(transform: &str, source_type: Option<&str>, value: &AvroValue) -> String {
    let iso = |time: DateTime<Utc>| time.to_rfc3339_opts(SecondsFormat::AutoSi, true);
    match (
        transform.split('[').next().unwrap_or(transform),
        source_type,
        value,
    ) {
        ("year", _, AvroValue::Int(years)) => format!("{:04}", 1970 + years),
        ("month", _, AvroValue::Int(months)) => format!(
            "{:04}-{:02}",
            1970 + months.div_euclid(12),
            months.rem_euclid(12) + 1
        ),
        ("day", _, AvroValue::Int(days))
        | ("" | "identity", Some("date"), AvroValue::Int(days)) => {
            civil_date(*days).unwrap_or_else(|| days.to_string())
        }
        ("hour", _, AvroValue::Int(hours)) => hours
            .checked_mul(3_600)
            .and_then(|secs| DateTime::from_timestamp(secs, 0))
            .map_or_else(
                || hours.to_string(),
                |time| time.format("%Y-%m-%d-%H").to_string(),
            ),
        ("" | "identity", Some("timestamp" | "timestamptz"), AvroValue::Int(micros)) => {
            DateTime::from_timestamp_micros(*micros).map_or_else(|| micros.to_string(), iso)
        }
        ("" | "identity", Some("timestamp_ms"), AvroValue::Int(millis)) => {
            DateTime::from_timestamp_millis(*millis).map_or_else(|| millis.to_string(), iso)
        }
        ("" | "identity", Some("timestamp_ns" | "timestamptz_ns"), AvroValue::Int(nanos)) => {
            iso(DateTime::from_timestamp_nanos(*nanos))
        }
        ("" | "identity", Some("uuid"), AvroValue::Bytes(bytes)) if bytes.len() == 16 => {
            let digits = hex(bytes);
            format!(
                "{}-{}-{}-{}-{}",
                &digits[0..8],
                &digits[8..12],
                &digits[12..16],
                &digits[16..20],
                &digits[20..32]
            )
        }
        ("" | "identity", Some("uuid"), AvroValue::Text(text)) => text.to_lowercase(),
        (_, Some(ty), AvroValue::Bytes(bytes)) if ty.starts_with("decimal(") => {
            decimal_text(ty, bytes).unwrap_or_else(|| hex(bytes))
        }
        (_, _, AvroValue::Int(number)) => number.to_string(),
        (_, _, AvroValue::Text(text)) => text.clone(),
        (_, _, AvroValue::Bytes(bytes)) => hex(bytes),
        (_, _, AvroValue::Null | AvroValue::Record(_)) => String::new(),
    }
}

fn civil_date(days: i64) -> Option<String> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?
        .checked_add_signed(Duration::days(days))
        .map(|date| date.format("%Y-%m-%d").to_string())
}

fn decimal_text(source_type: &str, bytes: &[u8]) -> Option<String> {
    let (_, scale) = source_type
        .strip_prefix("decimal(")?
        .strip_suffix(')')?
        .split_once(',')?;
    let scale: u32 = scale.trim().parse().ok()?;
    let unscaled = unscaled_value(bytes)?;
    Some(scaled_text(unscaled, scale))
}

fn unscaled_value(bytes: &[u8]) -> Option<i128> {
    (bytes.len() <= 16).then(|| {
        let sign = if bytes.first().is_some_and(|first| first & 0x80 != 0) {
            -1
        } else {
            0
        };
        bytes
            .iter()
            .fold(sign, |acc, &byte| (acc << 8) | i128::from(byte))
    })
}

fn scaled_text(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    let magnitude = unscaled.unsigned_abs();
    let divisor = 10_u128.pow(scale);
    let sign = if unscaled < 0 { "-" } else { "" };
    format!(
        "{sign}{}.{:0width$}",
        magnitude / divisor,
        magnitude % divisor,
        width = scale as usize
    )
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

#[derive(Clone)]
enum TableRoot {
    Local(PathBuf),
    Object(Url),
}

fn table_root(location: &str) -> Result<TableRoot, Error> {
    if !location.contains("://") {
        return Ok(TableRoot::Local(PathBuf::from(location)));
    }
    let mut url = Url::parse(location).map_err(Error::Uri)?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    if url.scheme() == "file" {
        return url.to_file_path().map(TableRoot::Local).map_err(|()| {
            Error::Invalid(format!(
                "cannot convert table location to a path: {location}"
            ))
        });
    }
    Ok(TableRoot::Object(url))
}

fn data_file_parts(root: &TableRoot, location: &str) -> Result<(String, String), Error> {
    let outside = || {
        Error::Invalid(format!(
            "active data file is outside the Iceberg table location: {location}"
        ))
    };
    match root {
        TableRoot::Local(root) => {
            let candidate = if location.contains("://") {
                let url = Url::parse(location).map_err(Error::Uri)?;
                if url.scheme() != "file" {
                    return Err(Error::Invalid(format!(
                        "active data file is not local: {location}"
                    )));
                }
                url.to_file_path().map_err(|()| {
                    Error::Invalid(format!(
                        "cannot convert active data file to a path: {location}"
                    ))
                })?
            } else {
                relative_data_path(location)?.to_path_buf()
            };
            let full = if candidate.is_absolute() {
                candidate
            } else {
                root.join(candidate)
            };
            let relative = full.strip_prefix(root).map_err(|_| outside())?;
            if !is_normal_relative(relative) {
                return Err(outside());
            }
            let slash = relative
                .components()
                .filter_map(|c| match c {
                    Component::Normal(part) => Some(part.to_string_lossy()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("/");
            Ok((full.to_string_lossy().into_owned(), slash))
        }
        TableRoot::Object(base) => {
            let url = if location.contains("://") {
                Url::parse(location).map_err(Error::Uri)?
            } else {
                relative_data_path(location)?;
                base.join(location).map_err(Error::Uri)?
            };
            if url.scheme() != base.scheme() || url.host_str() != base.host_str() {
                return Err(outside());
            }
            let relative =
                crate::v1::name::decode(url.path().strip_prefix(base.path()).ok_or_else(outside)?);
            if !is_normal_relative(Path::new(&relative)) {
                return Err(outside());
            }
            Ok((url.into(), relative))
        }
    }
}

fn is_normal_relative(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

async fn read_location(lake: &Lake, location: &str) -> Result<Vec<u8>, Error> {
    if location.contains("://") {
        let reader = object_store::open(lake, location).map_err(|e| Error::Storage(Box::new(e)))?;
        let stat = reader
            .stat()
            .await
            .map_err(|e| Error::Storage(Box::new(e)))?;
        reader
            .read_range(0..stat.size_bytes, stat.identity.as_deref())
            .await
            .map_err(|e| Error::Storage(Box::new(e)))
    } else {
        std::fs::read(Path::new(location)).map_err(|source| io_error(Path::new(location), source))
    }
}

fn io_error(path: &Path, source: std::io::Error) -> Error {
    Error::Io {
        path: path.display().to_string(),
        source,
    }
}

fn create_u64(value: i64) -> Result<u64, Error> {
    u64::try_from(value)
        .map_err(|_| Error::Invalid(format!("Iceberg snapshot id is negative: {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_resolve_and_reject_escapes() {
        let object = TableRoot::Object(Url::parse("s3://bucket/table/").unwrap());
        for input in ["s3://bucket/table/data/a%20b.parquet", "data/a%20b.parquet"] {
            let (uri, path) = data_file_parts(&object, input).unwrap();
            assert_eq!(uri, "s3://bucket/table/data/a%20b.parquet");
            assert_eq!(path, "data/a b.parquet");
        }
        assert!(data_file_parts(&object, "s3://bucket/other/a.parquet").is_err());
        assert!(data_file_parts(&object, "s3://other/table/a.parquet").is_err());

        let local = TableRoot::Local(PathBuf::from("/tmp/table"));
        assert_eq!(
            data_file_parts(&local, "data/a.parquet").unwrap(),
            ("/tmp/table/data/a.parquet".into(), "data/a.parquet".into())
        );
        assert!(data_file_parts(&local, "file:///tmp/other/a.parquet").is_err());
    }

    #[test]
    fn transforms_and_identity_types_format_values() {
        let uuid = vec![
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0x4d, 0xef, 0x8a, 0xbc, 0xde, 0xf0, 0x12, 0x34,
            0x56, 0x78,
        ];
        let cases = [
            ("year", None, AvroValue::Int(55), "2025"),
            ("month", None, AvroValue::Int(661), "2025-02"),
            ("month", None, AvroValue::Int(0), "1970-01"),
            ("day", Some("date"), AvroValue::Int(20_089), "2025-01-01"),
            (
                "day",
                Some("date"),
                AvroValue::Text("2025-01-01".into()),
                "2025-01-01",
            ),
            (
                "hour",
                Some("timestamp"),
                AvroValue::Int(481_321),
                "2024-11-28-01",
            ),
            (
                "identity",
                Some("timestamptz"),
                AvroValue::Int(1_735_689_600_000_000),
                "2025-01-01T00:00:00Z",
            ),
            (
                "identity",
                Some("uuid"),
                AvroValue::Bytes(uuid),
                "12345678-9abc-4def-8abc-def012345678",
            ),
            (
                "identity",
                Some("decimal(9, 2)"),
                AvroValue::Bytes(vec![0x30, 0x39]),
                "123.45",
            ),
            (
                "identity",
                Some("decimal(9, 2)"),
                AvroValue::Bytes(vec![0xfb]),
                "-0.05",
            ),
            (
                "identity",
                Some("binary"),
                AvroValue::Bytes(vec![0x0f, 0xff]),
                "0fff",
            ),
            ("bucket[16]", Some("long"), AvroValue::Int(7), "7"),
            (
                "truncate[3]",
                Some("string"),
                AvroValue::Text("abc".into()),
                "abc",
            ),
        ];
        for (transform, source, value, expected) in cases {
            assert_eq!(partition_text(transform, source, &value), expected);
        }
    }
}
