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
use crate::Workspace;

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

pub(crate) async fn get(workspace: &Workspace, name: &str) -> Result<Schema, Error> {
    let (catalog, schema) = crate::v1::name::parse_schema(name)
        .ok_or_else(|| Error::from(format!("{name} is not a catalogs/…/schemas/… name")))?;
    match workspace.config().dialect {
        TableFormat::Unity => unity_schema(workspace, &catalog, &schema).await,
        TableFormat::Iceberg => iceberg_schema(workspace, &catalog, &schema).await,
    }
}

async fn unity_schema(workspace: &Workspace, catalog: &str, schema: &str) -> Result<Schema, Error> {
    let full_name = format!("{catalog}.{schema}");
    let url = format!(
        "{}/schemas/{}",
        dialect::api_root(workspace.config().endpoint.as_str()),
        dialect::encode(&full_name)
    );
    let record: UnityRecord =
        dialect::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::from)?;
    let name = record
        .name
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| schema.to_string());
    Ok(Schema {
        name: crate::v1::name::schema(catalog, &name),
        comment: record.comment.filter(|comment| !comment.is_empty()),
        uri: record
            .storage_location
            .filter(|location| !location.is_empty()),
        properties: record.properties.unwrap_or_default(),
        ..Schema::default()
    })
}

async fn iceberg_schema(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
) -> Result<Schema, Error> {
    let url = format!(
        "{}/namespaces/{}",
        dialect::iceberg_root(workspace.config().endpoint.as_str()),
        dialect::iceberg_namespace(schema)
    );
    let namespace: Namespace =
        dialect::get_json(workspace, &url, workspace.config().token.as_deref())
            .await
            .map_err(Error::from)?;
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
        name: crate::v1::name::schema(catalog, &name),
        uri: location,
        properties,
        ..Schema::default()
    })
}
