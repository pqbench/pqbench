use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use serde_json::Value;

use super::json::{self, FromJson};
use super::name;
use super::Parent;

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

impl FromJson for Schema {
    fn from_json(value: Value) -> Result<Self, String> {
        json::record(
            value,
            "schema",
            "catalogs/…/schemas/…",
            Self::named,
            |schema| &schema.name,
            |schema| &mut schema.name,
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

impl Parent for Schema {
    const NOUN: &'static str = "schema";

    fn name(&self) -> &str {
        &self.name
    }
}
