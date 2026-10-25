use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A catalog's kind. Absence is `None` on the field, not a variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CatalogType {
    #[serde(rename = "CATALOG_TYPE_MANAGED")]
    Managed,
    #[serde(rename = "CATALOG_TYPE_FOREIGN")]
    Foreign,
    #[serde(rename = "CATALOG_TYPE_SYSTEM")]
    System,
}

impl CatalogType {
    /// The proto value name, as the wire spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Managed => "CATALOG_TYPE_MANAGED",
            Self::Foreign => "CATALOG_TYPE_FOREIGN",
            Self::System => "CATALOG_TYPE_SYSTEM",
        }
    }
}

/// The metastore: the entity above catalogs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Metastore {
    /// `metastores/{metastore}`.
    pub name: String,
    /// The system UUID, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The user-set metastore name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// The cloud the metastore lives in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_provider: Option<String>,
    /// The cloud region the metastore lives in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

/// One catalog.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Catalog {
    /// `catalogs/{catalog}`.
    pub name: String,
    /// The system UUID, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// A display name distinct from the id, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// The catalog kind, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_type: Option<CatalogType>,
    /// The catalog comment, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// The catalog owner, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

/// One schema.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Schema {
    /// `catalogs/{catalog}/schemas/{schema}`.
    pub name: String,
    /// The system UUID, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The schema comment, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// The schema's storage URI, when it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// The schema properties the endpoint reports.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
}

use serde_json::Value;

use super::json::{self, FromJson};
use super::name;
use super::Parent;

impl FromJson for Catalog {
    fn from_json(value: Value) -> Result<Self, String> {
        json::record(value, "catalog", "catalogs/…", Self::named, |catalog| {
            &catalog.name
        })
    }
}

impl Catalog {
    fn named(name: &str) -> Option<Self> {
        if name::parse_catalog(name).is_some() {
            return Some(Self {
                name: name.to_owned(),
                ..Self::default()
            });
        }
        if !name.is_empty() && !name.contains('.') {
            return Some(Self {
                name: name::catalog(name),
                ..Self::default()
            });
        }
        None
    }
}

impl FromJson for Schema {
    fn from_json(value: Value) -> Result<Self, String> {
        json::record(
            value,
            "schema",
            "catalogs/…/schemas/…",
            Self::named,
            |schema| &schema.name,
        )
    }
}

impl Schema {
    fn named(name: &str) -> Option<Self> {
        if name::parse_schema(name).is_some() {
            return Some(Self {
                name: name.to_owned(),
                ..Self::default()
            });
        }
        let (catalog, schema) = name.split_once('.')?;
        if catalog.is_empty() || schema.is_empty() {
            return None;
        }
        Some(Self {
            name: name::schema(catalog, schema),
            ..Self::default()
        })
    }
}

impl Parent for Catalog {
    const NOUN: &'static str = "catalog";

    fn name(&self) -> &str {
        &self.name
    }
}

impl Parent for Schema {
    const NOUN: &'static str = "schema";

    fn name(&self) -> &str {
        &self.name
    }
}
