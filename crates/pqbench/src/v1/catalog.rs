use serde::{Deserialize, Serialize};

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

impl Parent for Catalog {
    const NOUN: &'static str = "catalog";

    const SHAPE: &'static str = "catalogs/…";

    fn from_name(name: &str) -> Option<Self> {
        let catalog = name::parse_catalog(name)?;
        Some(Self {
            name: name::catalog(&catalog),
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
