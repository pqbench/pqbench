use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::json::{self, FromJson};
use super::name;
use super::CatalogType;
use super::Parent;

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

impl FromJson for Catalog {
    fn from_json(value: Value) -> Result<Self, String> {
        json::record(
            value,
            "catalog",
            "catalogs/…",
            Self::named,
            |catalog| &catalog.name,
            |catalog| &mut catalog.name,
        )
    }
}

impl Catalog {
    fn named(name: &str) -> Option<Self> {
        let catalog = name::parse_catalog(name)?;
        Some(Self {
            name: name::catalog(&catalog),
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
