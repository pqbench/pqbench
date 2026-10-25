use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

impl Parent for Schema {
    const NOUN: &'static str = "schema";

    const SHAPE: &'static str = "catalogs/…/schemas/…";

    fn from_name(name: &str) -> Option<Self> {
        let (catalog, schema) = name::parse_schema(name)?;
        Some(Self {
            name: name::schema(&catalog, &schema),
            ..Self::default()
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn name_mut(&mut self) -> &mut String {
        &mut self.name
    }
}
