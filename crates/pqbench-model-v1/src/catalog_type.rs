use serde::{Deserialize, Serialize};

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
