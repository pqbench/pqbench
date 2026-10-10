//! The Unity and Iceberg REST calls behind [`super::api::read`].
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`). Unity
//! serves `/tables/{catalog}.{schema}.{table}`; its `storage_location` is
//! loaded without files and the catalog's declared columns and properties are
//! merged over the log's. Iceberg REST serves
//! `{endpoint}/namespaces/{namespace}/tables/{table}` (`loadTable`), whose
//! response carries the metadata JSON inline: columns, partition spec,
//! snapshot, and properties are read from it, so no storage read runs. The
//! URLs and the JSON shapes are this command's; the transport is the
//! third-party facade.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use super::api::{Error, Storage, TableFormat as Dialect};
use crate::dialect;
use crate::table::{self, Column, LoadRequest, TableFormat, TableInfo};

pub(crate) async fn read(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
    table_format: Dialect,
    storage: &Storage,
) -> Result<TableInfo, Error> {
    match table_format {
        Dialect::Unity => unity_record(endpoint, catalog, schema, table, token, storage).await,
        Dialect::Iceberg => {
            iceberg_record(endpoint, catalog, schema, table, token, &storage.env).await
        }
    }
}

/// The subset of Unity `GET /tables/{full_name}` this command reports.
#[derive(Deserialize)]
struct UnityRecord {
    #[serde(default)]
    storage_location: Option<String>,
    #[serde(default)]
    columns: Vec<UnityColumn>,
    #[serde(default)]
    properties: Option<BTreeMap<String, String>>,
}

#[derive(Deserialize)]
struct UnityColumn {
    name: String,
    #[serde(default)]
    type_text: Option<String>,
    #[serde(default)]
    type_name: Option<String>,
    #[serde(default)]
    nullable: Option<bool>,
}

async fn unity_record(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
    storage: &Storage,
) -> Result<TableInfo, Error> {
    let name = format!("{catalog}.{schema}.{table}");
    let url = format!(
        "{}/tables/{}",
        dialect::api_root(endpoint),
        dialect::encode(&name)
    );
    // The parent ref already names the storage, so start the catalog call, read
    // the metadata while it is in flight, then reconcile: the catalog's own
    // location stays authoritative, and a ref that disagrees is read again.
    let (record, info) = match storage
        .location
        .as_deref()
        .filter(|location| !location.is_empty())
    {
        Some(location) => {
            let catalog = tokio::spawn({
                let url = url.clone();
                let token = token.map(str::to_owned);
                async move { dialect::get_json::<UnityRecord>(&url, token.as_deref()).await }
            });
            let info = load_metadata(&name, location, &storage.env).await;
            let record = catalog.await.map_err(task_error)?.map_err(Error::from)?;
            let catalog_location = catalog_location(&record, &name)?;
            let info = match catalog_location == location {
                true => info?,
                false => load_metadata(&name, &catalog_location, &storage.env).await?,
            };
            (record, info)
        }
        None => {
            let record: UnityRecord = dialect::get_json(&url, token).await.map_err(Error::from)?;
            let location = catalog_location(&record, &name)?;
            let info = load_metadata(&name, &location, &storage.env).await?;
            (record, info)
        }
    };
    let mut info = info;
    info.name = name;
    if !record.columns.is_empty() {
        info.columns = record
            .columns
            .into_iter()
            .map(|column| Column {
                name: column.name,
                data_type: column.type_text.or(column.type_name).unwrap_or_default(),
                nullable: column.nullable.unwrap_or(true),
            })
            .collect();
    }
    match info.format {
        TableFormat::DELTA => info
            .delta_properties
            .extend(record.properties.unwrap_or_default()),
        TableFormat::ICEBERG => {
            info.iceberg_properties
                .extend(record.properties.unwrap_or_default());
        }
        _ => {}
    }
    Ok(info)
}

/// The storage location the catalog names; an error when it names none.
fn catalog_location(record: &UnityRecord, name: &str) -> Result<String, Error> {
    record
        .storage_location
        .clone()
        .filter(|location| !location.is_empty())
        .ok_or_else(|| Error::from(format!("the table {name} has no storage location")))
}

/// The table's metadata at `location`, without files, naming the table and the
/// location on failure.
async fn load_metadata(
    name: &str,
    location: &str,
    env: &BTreeMap<String, String>,
) -> Result<TableInfo, Error> {
    let request = LoadRequest::new(location.to_string(), None, env.clone()).without_files();
    table::load(&request).await.map_err(|error| {
        let hint = if location.starts_with("s3://") {
            " (s3:// needs credentials on the lake source; Databricks default storage cannot be read outside Databricks compute)"
        } else {
            ""
        };
        Error::from(format!(
            "cannot read {name}'s metadata at {location}: {}{hint}",
            error.0
        ))
    })
}

/// A spawned metadata read that could not be joined.
fn task_error(error: tokio::task::JoinError) -> Error {
    Error::from(format!("the metadata read failed: {error}"))
}

/// The Iceberg REST `loadTable` subset this command reports: the metadata
/// JSON is inline, so no storage read runs.
#[derive(Deserialize)]
struct LoadedTable {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
    #[serde(default)]
    metadata: Option<IcebergMetadata>,
}

#[derive(Deserialize)]
struct IcebergMetadata {
    #[serde(rename = "current-snapshot-id", default)]
    current_snapshot_id: Option<i64>,
    #[serde(rename = "current-schema-id", default)]
    current_schema_id: i32,
    #[serde(rename = "default-spec-id", default)]
    default_spec_id: i32,
    #[serde(default)]
    schemas: Vec<IcebergSchema>,
    #[serde(rename = "partition-specs", default)]
    partition_specs: Vec<IcebergPartitionSpec>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct IcebergSchema {
    #[serde(rename = "schema-id")]
    schema_id: i32,
    #[serde(default)]
    fields: Vec<IcebergField>,
}

#[derive(Deserialize)]
struct IcebergField {
    name: String,
    #[serde(rename = "type")]
    data_type: Value,
    #[serde(default)]
    required: bool,
}

#[derive(Deserialize)]
struct IcebergPartitionSpec {
    #[serde(rename = "spec-id")]
    spec_id: i32,
    #[serde(default)]
    fields: Vec<IcebergPartitionField>,
}

#[derive(Deserialize)]
struct IcebergPartitionField {
    name: String,
}

async fn iceberg_record(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    token: Option<&str>,
    env: &BTreeMap<String, String>,
) -> Result<TableInfo, Error> {
    let name = format!("{catalog}.{schema}.{table}");
    let url = format!(
        "{}/namespaces/{}/tables/{}",
        dialect::iceberg_root(endpoint),
        dialect::iceberg_namespace(schema),
        dialect::encode(table)
    );
    let loaded: LoadedTable = dialect::get_json(&url, token).await.map_err(Error::from)?;
    if loaded.metadata_location.is_empty() {
        return Err(Error::from(format!(
            "the table {name} has no metadata location"
        )));
    }
    let metadata = loaded
        .metadata
        .ok_or_else(|| Error::from(format!("the table {name} has no metadata")))?;
    let columns = columns(&metadata);
    let partition_columns = partition_columns(&metadata);
    // The metadata JSON is what a later stage reads: an Iceberg table root is
    // only detectable when it carries `metadata/version-hint.text`, so the
    // storage path names the metadata JSON instead.
    let uri = loaded.metadata_location;
    let snapshot_version = metadata
        .current_snapshot_id
        .and_then(|snapshot_id| u64::try_from(snapshot_id).ok())
        .unwrap_or(0);
    let mut info = TableInfo::new(
        TableFormat::ICEBERG,
        uri,
        snapshot_version,
        partition_columns,
        Vec::new(),
        Vec::new(),
        env.clone(),
    );
    info.name = name;
    info.columns = columns;
    info.iceberg_properties = metadata.properties;
    Ok(info)
}

/// The columns of the current schema; the first schema when none is marked.
fn columns(metadata: &IcebergMetadata) -> Vec<Column> {
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

/// The partition fields of the default spec; the first spec when none is
/// marked.
fn partition_columns(metadata: &IcebergMetadata) -> Vec<String> {
    metadata
        .partition_specs
        .iter()
        .find(|spec| spec.spec_id == metadata.default_spec_id)
        .or_else(|| metadata.partition_specs.first())
        .map(|spec| spec.fields.iter().map(|field| field.name.clone()).collect())
        .unwrap_or_default()
}

/// A primitive type is its name; a nested type stays the format's JSON form.
fn type_name(value: &Value) -> String {
    match value {
        Value::String(name) => name.clone(),
        other => other.to_string(),
    }
}
