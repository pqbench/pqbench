use serde::{Deserialize, Serialize};

/// One column of a table's schema. Not a resource: a bounded value on its
/// [`Table`](super::Table), while a file's copy of it is the [`Column`](super::Column) resource.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DeclaredColumn {
    /// The column's name within the schema.
    pub column: String,
    /// The data type as the format reports it (e.g. `long`, `string`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
    /// Whether the column accepts nulls.
    #[serde(default)]
    pub nullable: bool,
    /// The column's position in the schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
}
