//! The Unity and Iceberg REST calls behind [`super::api::read`].
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`). Unity
//! serves `/schemas/{catalog}.{schema}`; Iceberg REST serves
//! `{endpoint}/namespaces/{namespace}` (`loadNamespace`), where the endpoint
//! already names the catalog base. No config probe runs. The URLs and the
//! JSON shapes are this command's; the transport is the third-party facade.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::api::{Error, Schema};
use crate::dialect;
use crate::schema::TableFormat;

/// The subset of Unity `GET /schemas/{full_name}` this command reports.
#[derive(Deserialize)]
struct UnityRecord {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    properties: Option<BTreeMap<String, String>>,
    #[serde(default)]
    storage_location: Option<String>,
}

/// The Iceberg REST `loadNamespace` document.
#[derive(Deserialize)]
struct Namespace {
    #[serde(default, rename = "namespace")]
    namespaces: Vec<String>,
    #[serde(default)]
    properties: Option<BTreeMap<String, String>>,
}

pub(crate) async fn read(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    token: Option<&str>,
    table_format: TableFormat,
) -> Result<Schema, Error> {
    match table_format {
        TableFormat::Unity => unity_schema(endpoint, catalog, schema, token).await,
        TableFormat::Iceberg => iceberg_schema(endpoint, catalog, schema, token).await,
    }
}

async fn unity_schema(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    token: Option<&str>,
) -> Result<Schema, Error> {
    let full_name = format!("{catalog}.{schema}");
    let url = format!(
        "{}/schemas/{}",
        dialect::api_root(endpoint),
        dialect::encode(&full_name)
    );
    let record: UnityRecord = dialect::get_json(&url, token).await.map_err(Error::from)?;
    Ok(Schema {
        catalog: catalog.to_string(),
        name: record
            .name
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| schema.to_string()),
        comment: record.comment.filter(|comment| !comment.is_empty()),
        location: record
            .storage_location
            .filter(|location| !location.is_empty()),
        properties: record.properties.unwrap_or_default(),
    })
}

async fn iceberg_schema(
    endpoint: &str,
    catalog: &str,
    schema: &str,
    token: Option<&str>,
) -> Result<Schema, Error> {
    let url = format!(
        "{}/namespaces/{}",
        dialect::iceberg_root(endpoint),
        dialect::iceberg_namespace(schema)
    );
    let namespace: Namespace = dialect::get_json(&url, token).await.map_err(Error::from)?;
    let mut properties = namespace.properties.unwrap_or_default();
    let location = properties
        .remove("location")
        .filter(|location| !location.is_empty());
    let name = if namespace.namespaces.is_empty() {
        schema.to_string()
    } else {
        namespace.namespaces.join(".")
    };
    Ok(Schema {
        catalog: catalog.to_string(),
        name,
        comment: None,
        location,
        properties,
    })
}
