//! Iceberg metadata loader: the only module that reads Iceberg metadata.
//!
//! [`load`] reads the metadata JSON and Avro manifests and returns a
//! [`TableInfo`]. It does not measure Parquet footers; pipe the document to
//! `bytemass`.
//!
//! A snapshot's manifest list names its manifests, and a manifest names its
//! data and delete files as Avro records. A data file's `partition` record is
//! interpreted against the table's partition spec: the spec gives each
//! partition field's transform (`day`, `bucket[16]`, …) and the source column
//! its type, so the record's own Avro value becomes the partition value a
//! reader can compare — a date, not a day count.
//!
//! Nothing here is behind a `#[cfg]`: the Avro reader and the object store
//! enforce the `iceberg` and `aws` features at runtime, so this module compiles
//! in every configuration and fails only when a read needs a disabled one.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use futures_util::stream::{self, Stream};
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
    let metadata = Arc::new(read_metadata(lake, &request.uri).await?);
    let requested = request.snapshot_version.map(create_i64).transpose()?;
    let selected = select_snapshot(&metadata, requested)?;
    let snapshot_version = selected
        .as_ref()
        .map(|snapshot| create_u64(snapshot.snapshot_id))
        .transpose()?
        .unwrap_or(0);
    let (files, deletes) = match selected {
        Some(snapshot) if request.require_files => {
            active_files(
                lake,
                &metadata,
                Arc::clone(&metadata),
                snapshot.snapshot_id,
                &snapshot.manifest_list,
            )
            .await?
        }
        _ => (Vec::new(), Vec::new()),
    };
    let mut log = if request.require_files {
        ancestry_log(&metadata, selected, &v1::name::local_table(&request.uri))?
    } else {
        Vec::new()
    };
    attach_actions(&mut log, snapshot_version, &files, &deletes);
    let mut info = TableInfo::new(
        v1::TableFormat::Iceberg,
        request.uri.clone(),
        snapshot_version,
        partition_columns(&metadata),
        log,
        files,
    );
    info.columns = columns(&metadata);
    info.iceberg_properties = metadata.properties.clone();
    Ok(info)
}

/// Resolve, read, parse, and validate the table's metadata JSON.
async fn read_metadata(lake: &Lake, uri: &str) -> Result<TableMetadata, Error> {
    let metadata_location = resolve_metadata_location(lake, uri).await?;
    let metadata_bytes = read_location(lake, &metadata_location).await?;
    let metadata: TableMetadata = serde_json::from_slice(&metadata_bytes).map_err(Error::Json)?;
    ensure_format_version(&metadata)?;
    Ok(metadata)
}

/// Reject a format version this build cannot read.
///
/// The three versions share one metadata document and one manifest format; v2
/// adds delete manifests and snapshot sequence numbers, v3 adds deletion
/// vectors and nanoseconds, and each is a superset of the last.
fn ensure_format_version(metadata: &TableMetadata) -> Result<(), Error> {
    if !(1..=VERSION_LAST).contains(&metadata.format_version) {
        return Err(Error::Invalid(format!(
            "unsupported Iceberg format version: {}",
            metadata.format_version
        )));
    }
    Ok(())
}

/// The selected snapshot's ancestry as log commits, one action per snapshot.
fn ancestry_log(
    metadata: &TableMetadata,
    selected: Option<&Snapshot>,
    table: &str,
) -> Result<Vec<v1::Commit>, Error> {
    ancestry(metadata, selected)
        .into_iter()
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
        .collect::<Result<Vec<_>, _>>()
}

/// Attach the snapshot's added and deleted files to its log commit.
fn attach_actions(
    log: &mut [v1::Commit],
    version: u64,
    files: &[v1::TableFile],
    deletes: &[String],
) {
    let Some(commit) = log.iter_mut().find(|commit| commit.version == version) else {
        return;
    };
    for file in files {
        commit.actions.push(v1::LogAction {
            kind: Some("add".into()),
            path: Some(file.path.clone()),
            size_bytes: file.size_bytes,
        });
    }
    for path in deletes {
        commit.actions.push(v1::LogAction {
            kind: Some("delete".into()),
            path: Some(path.clone()),
            size_bytes: None,
        });
    }
}

/// The metadata JSON, reduced to what a reader uses. The REST `loadTable`
/// response carries the same document inline, so it parses into this type too.
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

/// One Iceberg schema; the current one carries the table's columns.
#[derive(Debug, Deserialize)]
pub(super) struct Schema {
    #[serde(rename = "schema-id", default)]
    schema_id: i32,
    #[serde(default)]
    fields: Vec<Field>,
}

/// One Iceberg schema field.
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

/// One partition spec. A table that evolved its partitioning has several, and a
/// manifest names the one its entries were written under.
#[derive(Debug, Deserialize)]
pub(super) struct PartitionSpec {
    #[serde(rename = "spec-id", default)]
    spec_id: i32,
    #[serde(default)]
    fields: Vec<PartitionField>,
}

/// One partition field: the source column, the transform applied to it, and the
/// name the partition value is recorded under.
#[derive(Debug, Deserialize)]
pub(super) struct PartitionField {
    #[serde(rename = "source-id", default)]
    source_id: i32,
    /// The field's own id, unique within the table. The metadata names it so a
    /// writer can identify the field an evolved spec keeps, reused, or voids;
    /// a reader names the field by `name`.
    #[allow(dead_code)]
    #[serde(rename = "field-id", default)]
    field_id: i32,
    name: String,
    /// The transform's JSON name, `transform`, is the spec's keyword; the
    /// field is the value it holds.
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

/// One row of a snapshot's manifest list.
#[derive(Debug, Clone)]
struct ManifestFile {
    manifest_path: String,
    /// `0` for a data manifest, `1` for a delete manifest (v2 and later).
    content: i32,
    partition_spec_id: i32,
    /// The snapshot that added the manifest, when the list records one.
    added_snapshot_id: Option<i64>,
    /// How many of the manifest's entries are deleted. Absent in v1 when the
    /// writer omitted the summary counts.
    deleted_files_count: Option<i64>,
}

/// One manifest entry, as its Avro record names it.
struct EntryRecord {
    status: i32,
    snapshot_id: Option<i64>,
    /// `0` data, `1` position deletes, `2` equality deletes (v2 and later).
    content: i32,
    file_path: String,
    file_format: String,
    size_bytes: i64,
    record_count: i64,
    /// The partition record's fields, still in their Avro types.
    partition: Vec<(String, AvroValue)>,
}

const STATUS_DELETED: i32 = 2;
const CONTENT_DATA: i32 = 0;
const MANIFEST_DELETES: i32 = 1;

/// The highest format version this build reads.
const VERSION_LAST: i32 = 3;

/// The partition columns of the table, in the order a partition id uses.
///
/// That is the default spec's non-`void` fields, then any non-`void` field an
/// earlier or later spec added. Partition evolution keeps old files under the
/// spec they were written with, so a table whose spec grew still reports one
/// column list, and a file under an older spec is named by the same list.
pub(super) fn partition_columns(metadata: &TableMetadata) -> Vec<String> {
    let mut columns: Vec<String> = Vec::new();
    for spec in ordered_specs(metadata) {
        for field in &spec.fields {
            if field.transform == "void" || columns.iter().any(|column| column == &field.name) {
                continue;
            }
            columns.push(field.name.clone());
        }
    }
    columns
}

/// The specs, the default one first, then the rest in metadata order.
fn ordered_specs(metadata: &TableMetadata) -> Vec<&PartitionSpec> {
    let mut specs: Vec<&PartitionSpec> = Vec::with_capacity(metadata.partition_specs.len());
    let default = metadata
        .partition_specs
        .iter()
        .find(|spec| spec.spec_id == metadata.default_spec_id)
        .or_else(|| metadata.partition_specs.first());
    specs.extend(default);
    specs.extend(
        metadata
            .partition_specs
            .iter()
            .filter(|spec| default.is_none_or(|default| spec.spec_id != default.spec_id)),
    );
    specs
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

/// The current schema, or the first one when none is marked.
fn current_schema(metadata: &TableMetadata) -> Option<&Schema> {
    metadata
        .schemas
        .iter()
        .find(|schema| schema.schema_id == metadata.current_schema_id)
        .or_else(|| metadata.schemas.first())
}

/// The schema type of a field id, looked up through the current schema first
/// and then through any other, since a partition field may name a column a
/// later schema dropped.
fn source_type(metadata: &TableMetadata, source_id: i32) -> Option<String> {
    current_schema(metadata)
        .and_then(|schema| field_type(&schema.fields, source_id))
        .or_else(|| {
            metadata
                .schemas
                .iter()
                .find_map(|schema| field_type(&schema.fields, source_id))
        })
}

/// The schema type of a field id among `fields`, descending into structs.
fn field_type(fields: &[Field], id: i32) -> Option<String> {
    if let Some(field) = fields.iter().find(|field| field.id == id) {
        return Some(type_name(&field.data_type));
    }
    for field in fields {
        let JsonValue::Object(object) = &field.data_type else {
            continue;
        };
        let Some(JsonValue::Array(nested)) = object.get("fields") else {
            continue;
        };
        let Ok(nested) = serde_json::from_value::<Vec<Field>>(JsonValue::Array(nested.clone()))
        else {
            continue;
        };
        if let Some(found) = field_type(&nested, id) {
            return Some(found);
        }
    }
    None
}

/// A primitive type is its name; a nested type stays the format's JSON form.
fn type_name(value: &JsonValue) -> String {
    match value {
        JsonValue::String(name) => name.clone(),
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
        .ok_or_else(|| Error::Invalid(format!("Iceberg snapshot does not exist: {snapshot_id}")))
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
) -> Result<impl Stream<Item = Result<v1::Commit, Error>>, Error> {
    let metadata = read_metadata(lake, &request.uri).await?;
    let requested = request.snapshot_version.map(create_i64).transpose()?;
    let selected = select_snapshot(&metadata, requested)?;
    // `ancestry` walks from the selected snapshot back through its parents, so
    // the newest snapshot comes first.
    let mut commits = Vec::new();
    let table = v1::name::local_table(&request.uri);
    for snapshot in ancestry(&metadata, selected) {
        let version = create_u64(snapshot.snapshot_id)?;
        commits.push(v1::Commit {
            name: v1::name::commit_name(&table, &version.to_string()),
            version,
            commit_time: snapshot.snapshot_time,
            commit_info: None,
            actions: Vec::new(),
        });
    }
    Ok(stream::iter(commits.into_iter().map(Ok)))
}

/// Stream the files the selected snapshot's manifests name.
///
/// An Iceberg snapshot attaches its files only when it is resolved, and this read
/// resolves one snapshot — the requested one, or the latest — so the files are
/// that snapshot's. A request that names commits and does not name that snapshot
/// yields none: what an older snapshot added is a separate manifest read.
///
/// Each file carries the size the manifest declares, which `bytemass`
/// cross-checks against the footer.
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
    // `data_file` already resolved each blob and normalized its relative path.
    Ok(stream::iter(files.into_iter().map(Ok)))
}

fn ancestry<'a>(metadata: &'a TableMetadata, selected: Option<&'a Snapshot>) -> Vec<&'a Snapshot> {
    let Some(selected) = selected else {
        return Vec::new();
    };
    let mut seen = BTreeSet::new();
    let newest_first: Vec<&Snapshot> = std::iter::successors(Some(selected), |snapshot| {
        snapshot
            .parent_snapshot_id
            .filter(|parent| *parent != -1)
            .and_then(|id| {
                metadata
                    .snapshots
                    .iter()
                    .find(|snapshot| snapshot.snapshot_id == id)
            })
    })
    // A parent cycle would otherwise repeat the snapshots it names.
    .take_while(|snapshot| seen.insert(snapshot.snapshot_id))
    .collect();
    newest_first.into_iter().rev().collect()
}

async fn resolve_metadata_location(lake: &Lake, uri: &str) -> Result<String, Error> {
    if is_metadata_json(uri) {
        return Ok(uri.to_string());
    }
    if is_local(uri) {
        return local_metadata(&local_path(uri)?);
    }
    let hint = join_uri(uri, "metadata/version-hint.text")?;
    let bytes = read_location(lake, &hint).await?;
    let version = parse_hint_version(&String::from_utf8_lossy(&bytes))?;
    for name in hint_names(version) {
        let candidate = join_uri(uri, &format!("metadata/{name}"))?;
        if object_exists(lake, &candidate).await? {
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
    let hint = std::fs::read_to_string(&hint_path).map_err(|e| Error::Io {
        path: hint_path.display().to_string(),
        source: e,
    })?;
    let version = parse_hint_version(&hint)?;
    for name in hint_names(version) {
        let candidate = metadata.join(name);
        if candidate.is_file() {
            return Ok(Some(candidate.to_string_lossy().into_owned()));
        }
    }
    Err(Error::Invalid(format!(
        "version-hint.text is {version} but no matching metadata JSON is in {}",
        metadata.display()
    )))
}

/// The newest metadata JSON in the directory, by leading version.
fn newest_metadata(metadata: &Path) -> Result<String, Error> {
    let entries = std::fs::read_dir(metadata).map_err(|e| Error::Io {
        path: metadata.display().to_string(),
        source: e,
    })?;
    let mut found = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| Error::Io {
            path: metadata.display().to_string(),
            source: e,
        })?;
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
    metadata: &TableMetadata,
    manifest_metadata: Arc<TableMetadata>,
    snapshot_id: i64,
    manifest_list: &str,
) -> Result<(Vec<v1::TableFile>, Vec<String>), Error> {
    let root = table_root(&metadata.location)?;
    let entries: Vec<Entry> = active_entries(
        lake.clone(),
        manifest_metadata,
        snapshot_id,
        manifest_list.to_owned(),
    )
    .await?
    .try_collect()
    .await?;
    let mut files = Vec::with_capacity(entries.len());
    let mut deletes = Vec::new();
    for entry in entries {
        match entry {
            Entry::Data(file) => files.push(file),
            Entry::Delete(path) => {
                // A delete outside the table keeps the manifest's raw path in
                // the log; it is never read.
                deletes.push(
                    data_file_parts(&root, &path)
                        .map(|(_, relative)| relative)
                        .unwrap_or(path),
                );
            }
        }
    }
    Ok((files, deletes))
}

/// One entry the active manifests name.
///
/// A manifest's entries are built together, so the data variant's size is one
/// manifest's cost, not the walk's state; a `Box` would add an allocation to
/// every file to save a few hundred bytes of stack.
#[allow(clippy::large_enum_variant)]
enum Entry {
    /// A data file to measure.
    Data(v1::TableFile),
    /// A delete file's path: named in the log, never measured.
    Delete(String),
}

/// One manifest entry as an active entry, or `None` when the entry is deleted.
fn active_entry(
    metadata: &TableMetadata,
    manifest: &ManifestFile,
    fallback_snapshot_id: i64,
    root: &TableRoot,
    entry: &EntryRecord,
) -> Result<Option<Entry>, Error> {
    if entry.status == STATUS_DELETED {
        return Ok(None);
    }
    if entry_content(manifest, entry) != CONTENT_DATA {
        return Ok(Some(Entry::Delete(entry.file_path.clone())));
    }
    data_file(metadata, manifest, Some(fallback_snapshot_id), root, entry)
        .map(|file| Some(Entry::Data(file)))
}

/// Stream the active manifests' entries, without collecting them.
///
/// The manifest list is read once; each manifest is then read when the walk
/// reaches it, so a table holds one manifest's entries at a time however many
/// manifests its snapshot names.
async fn active_entries(
    lake: Lake,
    metadata: Arc<TableMetadata>,
    snapshot_id: i64,
    manifest_list: String,
) -> Result<impl Stream<Item = Result<Entry, Error>> + Send + 'static, Error> {
    let root = table_root(&metadata.location)?;
    let manifests = manifests(&lake, &manifest_list).await?;
    Ok(stream::iter(manifests)
        .then(move |manifest| {
            let lake = lake.clone();
            let metadata = Arc::clone(&metadata);
            let root = root.clone();
            async move {
                let mut entries = Vec::new();
                for entry in manifest_entries(&lake, &manifest).await? {
                    if let Some(entry) =
                        active_entry(&metadata, &manifest, snapshot_id, &root, &entry)?
                    {
                        entries.push(entry);
                    }
                }
                Ok(entries)
            }
        })
        .map_ok(|entries| stream::iter(entries.into_iter().map(Ok)))
        .try_flatten())
}

/// Stream the selected snapshot's active files, without collecting them.
pub(super) async fn stream_active_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<super::FileStream<'static>, Error> {
    let metadata = Arc::new(read_metadata(lake, &request.uri).await?);
    let requested = request.snapshot_version.map(create_i64).transpose()?;
    let Some(snapshot) = select_snapshot(&metadata, requested)? else {
        return Ok(stream::empty().boxed());
    };
    let snapshot_id = snapshot.snapshot_id;
    let manifest_list = snapshot.manifest_list.clone();
    Ok(
        active_entries(lake.clone(), metadata, snapshot_id, manifest_list)
            .await?
            .try_filter_map(|entry| async move {
                Ok(match entry {
                    Entry::Data(file) => Some(file),
                    Entry::Delete(_) => None,
                })
            })
            .boxed(),
    )
}

/// Stream the data files the selected snapshot no longer references.
///
/// A file leaves a table one of two ways, and both keep it billed until an
/// expire or a vacuum has run:
///
/// - the selected snapshot's data manifests name it with `status == DELETED`,
///   which is what a delete commit records; and
/// - only a retained older snapshot still names its manifest. The files of such
///   a manifest are superseded, and the metadata JSON keeps the snapshot that
///   reads them until its retention expires.
///
/// Historical manifests are deduplicated by path, and a manifest the selected
/// snapshot already names is skipped: what the current snapshot holds is not
/// removed. A table with one snapshot pays one manifest-list read and, when the
/// list says no entry is deleted, none of its data manifests.
pub(super) async fn stream_removed_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<super::FileStream<'static>, Error> {
    let metadata = read_metadata(lake, &request.uri).await?;
    let requested = request.snapshot_version.map(create_i64).transpose()?;
    let Some(snapshot) = select_snapshot(&metadata, requested)? else {
        return Ok(stream::empty().boxed());
    };
    let root = table_root(&metadata.location)?;
    let current = manifests(lake, &snapshot.manifest_list).await?;
    let mut removed = Vec::new();
    for manifest in &current {
        if !is_data_manifest(manifest) || !may_hold_deleted(manifest) {
            continue;
        }
        for entry in manifest_entries(lake, manifest).await? {
            if entry.status == STATUS_DELETED && entry_content(manifest, &entry) == CONTENT_DATA {
                removed.push(table_file(
                    &metadata,
                    manifest,
                    Some(snapshot.snapshot_id),
                    &root,
                    &entry,
                )?);
            }
        }
    }
    let mut read_lists: BTreeSet<String> = BTreeSet::from([snapshot.manifest_list.clone()]);
    let mut read_manifests: BTreeSet<String> = current
        .iter()
        .map(|manifest| manifest.manifest_path.clone())
        .collect();
    for other in &metadata.snapshots {
        if other.snapshot_id == snapshot.snapshot_id
            || !read_lists.insert(other.manifest_list.clone())
        {
            continue;
        }
        for manifest in manifests(lake, &other.manifest_list).await? {
            if !is_data_manifest(&manifest)
                || !read_manifests.insert(manifest.manifest_path.clone())
            {
                continue;
            }
            for entry in manifest_entries(lake, &manifest).await? {
                if entry_content(&manifest, &entry) == CONTENT_DATA {
                    removed.push(table_file(
                        &metadata,
                        &manifest,
                        Some(other.snapshot_id),
                        &root,
                        &entry,
                    )?);
                }
            }
        }
    }
    Ok(stream::iter(removed.into_iter().map(Ok)).boxed())
}

/// Stream the selected snapshot's delete files: the merge-on-read deletes that
/// still apply to its data.
///
/// A delete manifest's active entries cover position deletes (`content == 1`),
/// equality deletes (`content == 2`), and a v3 deletion vector, which is a
/// position delete in a Puffin file. Their format is the writer's choice —
/// Parquet, Avro, ORC, Puffin — so only the record's own `file_format` names it;
/// nothing here reads the body.
pub(super) async fn stream_delete_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<super::FileStream<'static>, Error> {
    let metadata = read_metadata(lake, &request.uri).await?;
    let requested = request.snapshot_version.map(create_i64).transpose()?;
    let Some(snapshot) = select_snapshot(&metadata, requested)? else {
        return Ok(stream::empty().boxed());
    };
    let root = table_root(&metadata.location)?;
    let mut files = Vec::new();
    for manifest in manifests(lake, &snapshot.manifest_list).await? {
        if is_data_manifest(&manifest) {
            continue;
        }
        for entry in manifest_entries(lake, &manifest).await? {
            if entry.status == STATUS_DELETED || entry_content(&manifest, &entry) == CONTENT_DATA {
                continue;
            }
            files.push(delete_file(
                &metadata,
                &manifest,
                Some(snapshot.snapshot_id),
                &root,
                &entry,
            )?);
        }
    }
    Ok(stream::iter(files.into_iter().map(Ok)).boxed())
}

#[derive(Clone, Copy)]
enum ManifestRole {
    Historical(i64),
    Current(i64),
}

/// Stream a snapshot's removed, active, and delete files while reading the
/// metadata JSON, each manifest list, and each manifest at most once.
///
/// Historical manifests come first, then the selected snapshot's manifests from
/// oldest to newest (with deleted entries ahead of active ones in each
/// manifest), so any removed entry for a path arrives before a live active
/// entry that cancels it.
pub(super) async fn stream_snapshot_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<SnapshotStream<'static>, Error> {
    let metadata = Arc::new(read_metadata(lake, &request.uri).await?);
    let requested = request.snapshot_version.map(create_i64).transpose()?;
    let Some(snapshot) = select_snapshot(&metadata, requested)? else {
        return Ok(stream::empty().boxed());
    };
    let snapshot_id = snapshot.snapshot_id;
    let root = table_root(&metadata.location)?;
    let current = manifests(lake, &snapshot.manifest_list).await?;
    let mut read_lists: BTreeSet<String> = BTreeSet::from([snapshot.manifest_list.clone()]);
    let mut seen_manifests: BTreeSet<String> = current
        .iter()
        .map(|manifest| manifest.manifest_path.clone())
        .collect();
    let mut work: Vec<(ManifestRole, ManifestFile)> = Vec::new();
    for other in &metadata.snapshots {
        if other.snapshot_id == snapshot_id || !read_lists.insert(other.manifest_list.clone()) {
            continue;
        }
        for manifest in manifests(lake, &other.manifest_list).await? {
            if is_data_manifest(&manifest) && seen_manifests.insert(manifest.manifest_path.clone())
            {
                work.push((ManifestRole::Historical(other.snapshot_id), manifest));
            }
        }
    }
    work.extend(
        current
            .into_iter()
            .rev()
            .map(|manifest| (ManifestRole::Current(snapshot_id), manifest)),
    );
    let lake = lake.clone();
    Ok(stream::iter(work)
        .then(move |(role, manifest)| {
            let lake = lake.clone();
            let metadata = Arc::clone(&metadata);
            let root = root.clone();
            async move { snapshot_manifest_files(&lake, &metadata, &root, role, &manifest).await }
        })
        .map_ok(|entries| stream::iter(entries.into_iter().map(Ok)))
        .try_flatten()
        .boxed())
}

async fn snapshot_manifest_files(
    lake: &Lake,
    metadata: &TableMetadata,
    root: &TableRoot,
    role: ManifestRole,
    manifest: &ManifestFile,
) -> Result<Vec<SnapshotFile>, Error> {
    let entries = manifest_entries(lake, manifest).await?;
    let mut removed = Vec::new();
    let mut rest = Vec::new();
    match role {
        ManifestRole::Historical(fallback_id) => {
            for entry in entries {
                if entry_content(manifest, &entry) == CONTENT_DATA {
                    removed.push(SnapshotFile::Removed(table_file(
                        metadata,
                        manifest,
                        Some(fallback_id),
                        root,
                        &entry,
                    )?));
                }
            }
        }
        ManifestRole::Current(fallback_id) => {
            for entry in entries {
                let content = entry_content(manifest, &entry);
                if entry.status == STATUS_DELETED {
                    if content == CONTENT_DATA {
                        removed.push(SnapshotFile::Removed(table_file(
                            metadata,
                            manifest,
                            Some(fallback_id),
                            root,
                            &entry,
                        )?));
                    }
                } else if content == CONTENT_DATA {
                    rest.push(SnapshotFile::Active(data_file(
                        metadata,
                        manifest,
                        Some(fallback_id),
                        root,
                        &entry,
                    )?));
                } else {
                    rest.push(SnapshotFile::Delete(delete_file(
                        metadata,
                        manifest,
                        Some(fallback_id),
                        root,
                        &entry,
                    )?));
                }
            }
        }
    }
    removed.extend(rest);
    Ok(removed)
}

/// One active data file as a `v1::TableFile`, checking it is Parquet.
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

/// One active delete file as a `v1::TableFile`.
///
/// A delete file is measured as a file, not decoded, so its format is not
/// checked: position and equality deletes are Parquet, Avro, or ORC, and a v3
/// deletion vector is Puffin.
fn delete_file(
    metadata: &TableMetadata,
    manifest: &ManifestFile,
    fallback_snapshot_id: Option<i64>,
    root: &TableRoot,
    entry: &EntryRecord,
) -> Result<v1::TableFile, Error> {
    table_file(metadata, manifest, fallback_snapshot_id, root, entry)
}

/// One manifest entry as a `v1::TableFile`: its path, size, partition values,
/// record count, and when the snapshot that added it was committed.
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
    Ok(v1::TableFile {
        blob: Some(v1::name::blob_name(&uri)),
        path: relative,
        size_bytes: Some(size),
        modification_time: modification_time(metadata, snapshot_id),
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

/// When the snapshot that added a file was committed, milliseconds since the
/// Unix epoch. An unassigned or unknown snapshot id has no time.
fn modification_time(metadata: &TableMetadata, snapshot_id: Option<i64>) -> Option<u64> {
    let snapshot_id = snapshot_id?;
    let snapshot = metadata
        .snapshots
        .iter()
        .find(|snapshot| snapshot.snapshot_id == snapshot_id)?;
    u64::try_from(snapshot.snapshot_time?).ok()
}

/// A manifest entry's content, resolved against its manifest.
///
/// A v1 manifest carries data files and has no per-entry content, so the
/// manifest's own content decides; a v2+ delete manifest names its content on
/// every entry.
fn entry_content(manifest: &ManifestFile, entry: &EntryRecord) -> i32 {
    if manifest.content == MANIFEST_DELETES && entry.content == CONTENT_DATA {
        MANIFEST_DELETES
    } else {
        entry.content
    }
}

fn is_data_manifest(manifest: &ManifestFile) -> bool {
    manifest.content == CONTENT_DATA
}

/// Whether a data manifest can hold a deleted entry.
///
/// A v2+ manifest list reports how many of a manifest's entries are deleted, so
/// a manifest that reports none is skipped without reading it. A v1 list does
/// not, so its manifests are always read.
fn may_hold_deleted(manifest: &ManifestFile) -> bool {
    manifest.deleted_files_count.is_none_or(|count| count > 0)
}

/// The spec a manifest's entries were written under, falling back to the
/// table's default spec.
fn manifest_spec<'a>(
    metadata: &'a TableMetadata,
    manifest: &ManifestFile,
) -> Option<&'a PartitionSpec> {
    metadata
        .partition_specs
        .iter()
        .find(|spec| spec.spec_id == manifest.partition_spec_id)
        .or_else(|| {
            metadata
                .partition_specs
                .iter()
                .find(|spec| spec.spec_id == metadata.default_spec_id)
        })
        .or_else(|| metadata.partition_specs.first())
}

/// Read a manifest list.
async fn manifests(lake: &Lake, manifest_list: &str) -> Result<Vec<ManifestFile>, Error> {
    let bytes = read_location(lake, manifest_list).await?;
    read_avro_values(&bytes, "manifest list")
        .map_err(Error::Avro)?
        .into_iter()
        .map(manifest_record)
        .collect()
}

/// One manifest-list row's Avro record as a [`ManifestFile`].
///
/// Decoding from [`AvroValue`] rather than `serde` accepts both v1 schemas
/// (where `added_snapshot_id` and `deleted_data_files_count` may be nullable
/// unions) and v2/v3 schemas (where `added_snapshot_id` and
/// `deleted_files_count` are plain non-union integers).
fn manifest_record(value: AvroValue) -> Result<ManifestFile, Error> {
    let AvroValue::Record(fields) = value else {
        return Err(Error::Invalid(
            "an Iceberg manifest-list entry is not a record".into(),
        ));
    };
    let manifest_path = string_field(&fields, "manifest_path").ok_or_else(|| {
        Error::Invalid("an Iceberg manifest-list entry names no manifest_path".into())
    })?;
    let deleted_files_count = long_field(&fields, "deleted_files_count")
        .or_else(|| long_field(&fields, "deleted_data_files_count"));
    Ok(ManifestFile {
        manifest_path,
        content: int_field(&fields, "content").unwrap_or_default(),
        partition_spec_id: int_field(&fields, "partition_spec_id").unwrap_or_default(),
        added_snapshot_id: long_field(&fields, "added_snapshot_id"),
        deleted_files_count,
    })
}

/// Read a manifest's entries.
async fn manifest_entries(lake: &Lake, manifest: &ManifestFile) -> Result<Vec<EntryRecord>, Error> {
    let bytes = read_location(lake, &manifest.manifest_path).await?;
    read_avro_values(&bytes, "manifest")
        .map_err(Error::Avro)?
        .into_iter()
        .map(entry_record)
        .collect()
}

/// One entry's Avro record as an [`EntryRecord`].
fn entry_record(value: AvroValue) -> Result<EntryRecord, Error> {
    let AvroValue::Record(fields) = value else {
        return Err(Error::Invalid(
            "an Iceberg manifest entry is not a record".into(),
        ));
    };
    let data_file = record_field_value(&fields, "data_file")
        .filter(|value| matches!(value, AvroValue::Record(_)))
        .ok_or_else(|| Error::Invalid("an Iceberg manifest entry names no data_file".into()))?;
    let AvroValue::Record(data_file) = data_file else {
        return Err(Error::Invalid(
            "an Iceberg manifest entry's data_file is not a record".into(),
        ));
    };
    let partition = match record_field_value(data_file, "partition") {
        Some(AvroValue::Record(partition)) => partition.clone(),
        _ => Vec::new(),
    };
    Ok(EntryRecord {
        status: int_field(&fields, "status").unwrap_or_default(),
        snapshot_id: long_field(&fields, "snapshot_id"),
        content: int_field(data_file, "content").unwrap_or_default(),
        file_path: string_field(data_file, "file_path").unwrap_or_default(),
        file_format: string_field(data_file, "file_format").unwrap_or_default(),
        size_bytes: long_field(data_file, "file_size_in_bytes").unwrap_or_default(),
        record_count: long_field(data_file, "record_count").unwrap_or(-1),
        partition,
    })
}

fn record_field_value<'a>(fields: &'a [(String, AvroValue)], name: &str) -> Option<&'a AvroValue> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value)
}

fn int_field(fields: &[(String, AvroValue)], name: &str) -> Option<i32> {
    let value = record_field_value(fields, name)?;
    let raw = integer_value(value)?;
    i32::try_from(raw).ok()
}

fn long_field(fields: &[(String, AvroValue)], name: &str) -> Option<i64> {
    integer_value(record_field_value(fields, name)?)
}

fn string_field(fields: &[(String, AvroValue)], name: &str) -> Option<String> {
    match record_field_value(fields, name)? {
        AvroValue::String(text) => Some(text.clone()),
        _ => None,
    }
}

/// An Avro value's integer, when it has one: an `int`, a `long`, or the count
/// behind a `date`.
fn integer_value(value: &AvroValue) -> Option<i64> {
    match value {
        AvroValue::Int(number) => Some(i64::from(*number)),
        AvroValue::Long(number) => Some(*number),
        AvroValue::Date(days) => Some(i64::from(*days)),
        _ => None,
    }
}

/// A partition record's values, formatted as the spec says they mean.
///
/// The field a value sits under fixes its transform, and the transform's source
/// column fixes its type: the record's `20123` is `2025-02-02` under a `day`
/// transform on a `date` column and `12345` under `identity` on a `long`. A
/// `void` transform keeps its field in the spec for older files only, so a null
/// under it is not a partition value at all.
fn partition_values(
    metadata: &TableMetadata,
    manifest: &ManifestFile,
    partition: &[(String, AvroValue)],
) -> BTreeMap<String, Option<String>> {
    let spec = manifest_spec(metadata, manifest);
    let mut values = BTreeMap::new();
    for (name, value) in partition {
        let field = spec.and_then(|spec| spec.fields.iter().find(|field| &field.name == name));
        let transform = field.map_or("identity", |field| field.transform.as_str());
        if transform == "void" && matches!(value, AvroValue::Null) {
            continue;
        }
        let source_type = field.and_then(|field| source_type(metadata, field.source_id));
        let text = match value {
            AvroValue::Null => None,
            value => Some(partition_text(transform, source_type.as_deref(), value)),
        };
        values.insert(name.clone(), text);
    }
    values
}

/// One partition value as text, from its transform and its source column type.
fn partition_text(transform: &str, source_type: Option<&str>, value: &AvroValue) -> String {
    // A parameterized transform (`bucket[16]`, `truncate[4]`) is known by its
    // name: the parameter changes which bucket, not what the value means.
    let transform = transform.split('[').next().unwrap_or(transform);
    match transform {
        "year" => integer_value(value)
            .map(|years| format!("{:04}", 1970 + years))
            .unwrap_or_else(|| scalar_text(value)),
        "month" => integer_value(value)
            .map(|months| {
                format!(
                    "{:04}-{:02}",
                    1970 + months.div_euclid(12),
                    months.rem_euclid(12) + 1
                )
            })
            .unwrap_or_else(|| scalar_text(value)),
        "day" => date_text(value),
        "hour" => integer_value(value)
            .and_then(|hours| hours.checked_mul(3_600))
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
            .map(|time| time.format("%Y-%m-%d-%H").to_string())
            .unwrap_or_else(|| scalar_text(value)),
        // `identity` keeps the source column's value, so its type does the
        // formatting. Any other transform yields a plain count, string, or
        // bytes — a bucket id, a truncated prefix — with no further meaning.
        "" | "identity" => identity_text(source_type, value),
        _ => source_type
            .and_then(|source_type| decimal_text(source_type, value))
            .unwrap_or_else(|| scalar_text(value)),
    }
}

/// An `identity` partition value in its source column's terms.
fn identity_text(source_type: Option<&str>, value: &AvroValue) -> String {
    match source_type {
        Some("date") => date_text(value),
        Some("timestamp" | "timestamptz" | "timestamp_ns" | "timestamptz_ns" | "timestamp_ms") => {
            timestamp_text(value).unwrap_or_else(|| scalar_text(value))
        }
        Some("uuid") => uuid_text(value),
        Some(source_type) => decimal_text(source_type, value).unwrap_or_else(|| scalar_text(value)),
        None => scalar_text(value),
    }
}

/// A `date` value as `%Y-%m-%d`: its day count converted by the civil calendar,
/// or the text a writer already stored.
fn date_text(value: &AvroValue) -> String {
    match value {
        AvroValue::String(text) => text.clone(),
        value => integer_value(value)
            .and_then(civil_date)
            .unwrap_or_else(|| scalar_text(value)),
    }
}

/// The calendar date `days` after the Unix epoch.
fn civil_date(days: i64) -> Option<String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let date = epoch.checked_add_signed(chrono::Duration::days(days))?;
    Some(date.format("%Y-%m-%d").to_string())
}

/// A timestamp value as ISO-8601 in UTC. A bare `long` is microseconds, which is
/// how an Avro writer records an Iceberg `timestamp` without a logical type.
fn timestamp_text(value: &AvroValue) -> Option<String> {
    match value {
        AvroValue::TimestampMicros(micros) | AvroValue::Long(micros) => {
            DateTime::from_timestamp_micros(*micros).map(iso8601)
        }
        AvroValue::TimestampMillis(millis) => DateTime::from_timestamp_millis(*millis).map(iso8601),
        AvroValue::TimestampNanos(nanos) => Some(iso8601(DateTime::from_timestamp_nanos(*nanos))),
        _ => None,
    }
}

fn iso8601(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// A `decimal(P, S)` value as a scaled decimal string.
///
/// The Avro bytes are the unscaled value in big-endian two's complement, so the
/// scale is the only thing the type adds: `12345` at scale 2 is `123.45` and
/// `-5` at scale 2 is `-0.05`. `None` when the type is not a decimal or the
/// value is not its bytes.
fn decimal_text(source_type: &str, value: &AvroValue) -> Option<String> {
    let scale = decimal_scale(source_type)?;
    let bytes = match value {
        AvroValue::Decimal(bytes) | AvroValue::Bytes(bytes) => bytes,
        _ => return None,
    };
    let unscaled = unscaled_value(bytes)?;
    Some(scaled_text(unscaled, scale))
}

/// The scale of a `decimal(P, S)` type name.
fn decimal_scale(source_type: &str) -> Option<u32> {
    let parameters = source_type.strip_prefix("decimal(")?.strip_suffix(')')?;
    let (_, scale) = parameters.split_once(',')?;
    scale.trim().parse().ok()
}

/// An arbitrary-width big-endian two's complement integer, up to `i128`.
fn unscaled_value(bytes: &[u8]) -> Option<i128> {
    if bytes.len() > 16 {
        return None;
    }
    let negative = bytes.first().is_some_and(|first| first & 0x80 != 0);
    let mut unscaled: i128 = if negative { -1 } else { 0 };
    for byte in bytes {
        unscaled = (unscaled << 8) | i128::from(*byte);
    }
    Some(unscaled)
}

/// A scaled decimal: the integer's digits with a decimal point `scale` from the
/// right, keeping the sign even when the whole part is zero.
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

/// A `uuid` value as canonical hyphenated lowercase text.
fn uuid_text(value: &AvroValue) -> String {
    match value {
        AvroValue::Uuid(text) => text.to_lowercase(),
        AvroValue::Bytes(bytes) | AvroValue::Decimal(bytes) if bytes.len() == 16 => {
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
        value => scalar_text(value),
    }
}

/// A value with no transform or column type to interpret it: the value's own
/// text, with bytes in lowercase hex.
fn scalar_text(value: &AvroValue) -> String {
    match value {
        AvroValue::Null => String::new(),
        AvroValue::Bool(value) => value.to_string(),
        AvroValue::Int(value) => value.to_string(),
        AvroValue::Long(value) => value.to_string(),
        AvroValue::Float(value) => value.to_string(),
        AvroValue::Double(value) => value.to_string(),
        AvroValue::String(value) => value.clone(),
        AvroValue::Bytes(value) => hex(value),
        AvroValue::Date(days) => civil_date(i64::from(*days)).unwrap_or_else(|| days.to_string()),
        AvroValue::TimestampMicros(micros) => {
            timestamp_text(value).unwrap_or_else(|| micros.to_string())
        }
        AvroValue::TimestampMillis(millis) => {
            timestamp_text(value).unwrap_or_else(|| millis.to_string())
        }
        AvroValue::TimestampNanos(value) => {
            DateTime::from_timestamp_nanos(*value).to_rfc3339_opts(SecondsFormat::AutoSi, true)
        }
        AvroValue::TimeMicros(value) => value.to_string(),
        AvroValue::TimeMillis(value) => value.to_string(),
        AvroValue::Decimal(value) => hex(value),
        AvroValue::Uuid(value) => value.to_lowercase(),
        AvroValue::Record(fields) => {
            let pairs: Vec<String> = fields
                .iter()
                .map(|(name, value)| format!("{name}={}", scalar_text(value)))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
        AvroValue::Array(items) => {
            let items: Vec<String> = items.iter().map(scalar_text).collect();
            format!("[{}]", items.join(", "))
        }
        AvroValue::Map(entries) => {
            let pairs: Vec<String> = entries
                .iter()
                .map(|(key, value)| format!("{key}={}", scalar_text(value)))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
    }
}

/// Bytes in lowercase hex.
fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(text, "{byte:02x}");
    }
    text
}

#[derive(Clone)]
enum TableRoot {
    Local(PathBuf),
    Object(Url),
}

fn table_root(location: &str) -> Result<TableRoot, Error> {
    if looks_like_uri(location) {
        let url = parse_dir_url(location)?;
        if url.scheme() == "file" {
            let path = url.to_file_path().map_err(|()| {
                Error::Invalid(format!(
                    "cannot convert table location to a path: {location}"
                ))
            })?;
            return Ok(TableRoot::Local(path));
        }
        return Ok(TableRoot::Object(url));
    }
    Ok(TableRoot::Local(PathBuf::from(location)))
}

/// The file's readable URI and its path relative to the table root.
///
/// Iceberg manifests may name a file either relative to the table location or
/// by full URI; a `TableFile` records the relative form (as Delta does) and
/// keeps the resolved URI only on its `Blob`.
fn data_file_parts(root: &TableRoot, location: &str) -> Result<(String, String), Error> {
    match root {
        TableRoot::Local(root) => {
            let path = local_data_path(root, location)?;
            let relative = path.strip_prefix(root).map_err(|_| {
                Error::Invalid(format!(
                    "active data file is outside the Iceberg table location: {location}"
                ))
            })?;
            if !relative
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
            {
                return Err(Error::Invalid(format!(
                    "active data file is outside the Iceberg table location: {location}"
                )));
            }
            Ok((path.to_string_lossy().into_owned(), slash_path(relative)))
        }
        TableRoot::Object(base) => {
            let uri = object_input(base, location)?;
            let url = Url::parse(&uri).map_err(Error::Uri)?;
            let relative = url.path().strip_prefix(base.path()).ok_or_else(|| {
                Error::Invalid(format!(
                    "active data file is outside the Iceberg table location: {location}"
                ))
            })?;
            let relative = crate::v1::name::decode(relative);
            if relative.is_empty()
                || !Path::new(&relative)
                    .components()
                    .all(|component| matches!(component, Component::Normal(_)))
            {
                return Err(Error::Invalid(format!(
                    "active data file is outside the Iceberg table location: {location}"
                )));
            }
            Ok((url.to_string(), relative))
        }
    }
}

/// A relative filesystem path with `/` separators, the form a resource id uses.
fn slash_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn local_data_path(root: &Path, location: &str) -> Result<PathBuf, Error> {
    let candidate = if looks_like_uri(location) {
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
    let local = if candidate.is_absolute() {
        candidate
    } else {
        root.join(candidate)
    };
    if !local.starts_with(root) {
        return Err(Error::Invalid(format!(
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
        base.join(location).map_err(Error::Uri)?.into()
    };
    let url = Url::parse(&uri).map_err(Error::Uri)?;
    if url.scheme() != base.scheme()
        || url.host_str() != base.host_str()
        || !url.path().starts_with(base.path())
    {
        return Err(Error::Invalid(format!(
            "active data file is outside the Iceberg table location: {location}"
        )));
    }
    Ok(uri)
}

fn parse_dir_url(location: &str) -> Result<Url, Error> {
    let mut url = Url::parse(location).map_err(Error::Uri)?;
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

async fn read_location(lake: &Lake, location: &str) -> Result<Vec<u8>, Error> {
    if looks_like_uri(location) {
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
        std::fs::read(location).map_err(|e| Error::Io {
            path: location.to_string(),
            source: e,
        })
    }
}

async fn object_exists(lake: &Lake, location: &str) -> Result<bool, Error> {
    let reader = object_store::open(lake, location).map_err(|e| Error::Storage(Box::new(e)))?;
    reader
        .exists()
        .await
        .map_err(|e| Error::Storage(Box::new(e)))
}

fn is_metadata_json(uri: &str) -> bool {
    uri.rsplit(['/', '\\'])
        .next()
        .is_some_and(|name| name.ends_with(".metadata.json"))
}

fn looks_like_uri(value: &str) -> bool {
    value.contains("://")
}

fn create_u64(value: i64) -> Result<u64, Error> {
    u64::try_from(value)
        .map_err(|_| Error::Invalid(format!("Iceberg snapshot id is negative: {value}")))
}

fn create_i64(value: u64) -> Result<i64, Error> {
    i64::try_from(value)
        .map_err(|_| Error::Invalid(format!("Iceberg snapshot id is too large: {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_paths_are_relative_decoded_and_canonical() {
        let root = TableRoot::Object(Url::parse("s3://bucket/table/").unwrap());
        let (uri, path) = data_file_parts(&root, "s3://bucket/table/data/a%20b.parquet").unwrap();
        assert_eq!(uri, "s3://bucket/table/data/a%20b.parquet");
        assert_eq!(path, "data/a b.parquet");

        let (uri, path) = data_file_parts(&root, "data/a%20b.parquet").unwrap();
        assert_eq!(uri, "s3://bucket/table/data/a%20b.parquet");
        assert_eq!(path, "data/a b.parquet");
    }

    #[test]
    fn object_paths_outside_the_table_are_rejected() {
        let root = TableRoot::Object(Url::parse("s3://bucket/table/").unwrap());
        assert!(data_file_parts(&root, "s3://bucket/other/a.parquet").is_err());
        assert!(data_file_parts(&root, "s3://other/table/a.parquet").is_err());
    }

    #[test]
    fn local_paths_are_relative_to_the_table_root() {
        let root = TableRoot::Local(PathBuf::from("/tmp/table"));
        let (uri, path) = data_file_parts(&root, "data/a.parquet").unwrap();
        assert_eq!(uri, "/tmp/table/data/a.parquet");
        assert_eq!(path, "data/a.parquet");
    }

    #[test]
    fn local_paths_outside_the_table_are_rejected() {
        let root = TableRoot::Local(PathBuf::from("/tmp/table"));
        assert!(data_file_parts(&root, "file:///tmp/other/a.parquet").is_err());
    }

    #[test]
    fn a_decimal_is_its_unscaled_value_at_the_types_scale() {
        assert_eq!(scaled_text(12_345, 2), "123.45");
        assert_eq!(scaled_text(-5, 2), "-0.05");
        assert_eq!(scaled_text(-12_300, 2), "-123.00");
        assert_eq!(scaled_text(7, 0), "7");
        assert_eq!(decimal_scale("decimal(38, 9)"), Some(9));
        assert_eq!(decimal_scale("long"), None);
    }

    #[test]
    fn big_endian_twos_complement_keeps_the_sign() {
        assert_eq!(unscaled_value(&[]), Some(0));
        assert_eq!(unscaled_value(&[0x30, 0x39]), Some(12_345));
        assert_eq!(unscaled_value(&[0xfb]), Some(-5));
        assert_eq!(unscaled_value(&[0xff, 0xff, 0xff, 0xfb]), Some(-5));
        assert_eq!(unscaled_value(&[0x00, 0xfb]), Some(251));
    }

    #[test]
    fn a_date_count_is_a_calendar_date() {
        assert_eq!(civil_date(0).as_deref(), Some("1970-01-01"));
        assert_eq!(civil_date(20_089).as_deref(), Some("2025-01-01"));
        assert_eq!(civil_date(-1).as_deref(), Some("1969-12-31"));
    }

    #[test]
    fn an_hour_count_is_an_hour_of_a_day() {
        assert_eq!(
            partition_text("hour", Some("timestamp"), &AvroValue::Int(0)),
            "1970-01-01-00"
        );
        assert_eq!(
            partition_text("hour", Some("timestamp"), &AvroValue::Long(481_321)),
            "2024-11-28-01"
        );
    }

    #[test]
    fn a_year_and_a_month_transform_count_from_1970() {
        assert_eq!(partition_text("year", None, &AvroValue::Int(55)), "2025");
        assert_eq!(
            partition_text("month", None, &AvroValue::Int(661)),
            "2025-02"
        );
        assert_eq!(partition_text("month", None, &AvroValue::Int(0)), "1970-01");
    }

    #[test]
    fn a_day_transform_keeps_a_string_and_converts_a_count() {
        assert_eq!(
            partition_text("day", Some("date"), &AvroValue::Date(20_089)),
            "2025-01-01"
        );
        assert_eq!(
            partition_text("day", Some("date"), &AvroValue::String("2025-01-01".into())),
            "2025-01-01"
        );
    }

    #[test]
    fn an_identity_value_follows_its_source_type() {
        let timestamp = 1_735_689_600_000_000;
        assert_eq!(
            partition_text(
                "identity",
                Some("timestamptz"),
                &AvroValue::TimestampMicros(timestamp)
            ),
            "2025-01-01T00:00:00Z"
        );
        let uuid = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0x4d, 0xef, 0x8a, 0xbc, 0xde, 0xf0, 0x12, 0x34,
            0x56, 0x78,
        ];
        assert_eq!(
            partition_text("identity", Some("uuid"), &AvroValue::Bytes(uuid.to_vec())),
            "12345678-9abc-4def-8abc-def012345678"
        );
        assert_eq!(
            partition_text("identity", Some("boolean"), &AvroValue::Bool(true)),
            "true"
        );
        assert_eq!(
            partition_text(
                "identity",
                Some("binary"),
                &AvroValue::Bytes(vec![0x0f, 0xff])
            ),
            "0fff"
        );
    }

    #[test]
    fn a_bucket_and_a_truncate_value_stay_scalars() {
        assert_eq!(
            partition_text("bucket[16]", Some("long"), &AvroValue::Int(7)),
            "7"
        );
        assert_eq!(
            partition_text(
                "truncate[3]",
                Some("string"),
                &AvroValue::String("abc".into())
            ),
            "abc"
        );
    }
}
