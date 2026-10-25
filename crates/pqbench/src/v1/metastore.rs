use serde::{Deserialize, Serialize};

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
