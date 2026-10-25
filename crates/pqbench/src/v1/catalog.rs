use serde::{Deserialize, Serialize};

use super::CatalogType;
use super::Resource;

/// One catalog.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[resource(noun = "catalog")]
#[serde(default)]
pub struct Catalog {
    /// `catalogs/{catalog}`.
    #[resource(name = "catalogs/{catalog}")]
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
