use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{Credentials, DeclaredColumn, Resource, TableFormat};

/// One table's record.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct Table {
    /// `catalogs/{catalog}/schemas/{schema}/tables/{table}`.
    #[resource(
        name = "catalogs/{catalog}/schemas/{schema}/tables/{table}",
        alias = "{catalog}.{schema}.{table}"
    )]
    pub name: String,
    /// The system UUID, when the endpoint reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    /// The storage root URI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// The detected table format, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<TableFormat>,
    /// Snapshot version the record describes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_version: Option<u64>,
    /// Partition columns; they need not occupy Parquet columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_columns: Vec<String>,
    /// The schema, in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declared_columns: Vec<DeclaredColumn>,
    /// The table's properties (Delta configuration, Iceberg properties).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
    /// Active data files in the snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_count: Option<u64>,
    /// The snapshot's partitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<u64>,
    /// The vended storage credentials, when the caller holds them. Held on the
    /// table so a downstream stage reads the table's own lease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Resource, Table};

    #[test]
    fn a_dotted_table_name_parses() {
        assert_eq!(
            Table::parse_name("a.b.c"),
            Some(("a".to_string(), "b".to_string(), "c".to_string()))
        );
    }

    #[test]
    fn a_dotted_object_name_is_normalized() {
        let table = Table::from_json(json!({"name": "a.b.c", "uri": "/lake/c"})).unwrap();
        assert_eq!(table.name, "catalogs/a/schemas/b/tables/c");
        assert_eq!(table.uri.as_deref(), Some("/lake/c"));
    }

    #[test]
    fn a_canonical_object_name_is_kept() {
        let table = Table::from_json(json!({"name": "catalogs/a/schemas/b/tables/c"})).unwrap();
        assert_eq!(table.name, "catalogs/a/schemas/b/tables/c");
    }

    #[test]
    fn a_record_round_trips_through_json() {
        let table = Table::from_json(json!({"name": "a.b.c", "uri": "/lake/c"})).unwrap();
        assert_eq!(
            table.to_json().unwrap()["name"],
            "catalogs/a/schemas/b/tables/c"
        );
    }
}
