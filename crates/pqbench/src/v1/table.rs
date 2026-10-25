use super::Credentials;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// An on-disk table format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TableFormat {
    #[serde(rename = "TABLE_FORMAT_DELTA")]
    Delta,
    #[serde(rename = "TABLE_FORMAT_ICEBERG")]
    Iceberg,
}

impl TableFormat {
    /// The proto value name, as the wire spells it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "TABLE_FORMAT_DELTA",
            Self::Iceberg => "TABLE_FORMAT_ICEBERG",
        }
    }
}

/// One table's record.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Table {
    /// `catalogs/{catalog}/schemas/{schema}/tables/{table}`.
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

/// One column of a table's schema. Not a resource: a bounded value on its
/// [`Table`], while a file's copy of it is the [`Column`] resource.
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

/// A natural partition: a half-open commit-time window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Partition {
    /// `…/tables/{table}/partitions/{partition}`.
    pub name: String,
    /// Inclusive start of the window, milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<u64>,
    /// Exclusive end of the window, milliseconds since the Unix epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<u64>,
    /// Commits in the window, in version order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_versions: Vec<u64>,
    /// The commits' `commitInfo` actions, index-aligned with
    /// [`commit_versions`](Self::commit_versions): the operation, its
    /// parameters and metrics, and the writer's identity, as the log stores
    /// them. Empty when no commit in the window has one — Iceberg carries none,
    /// and a Delta writer may leave it out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commit_info: Vec<serde_json::Value>,
    /// The table's storage root URI. Denormalized onto the partition so the
    /// files stage reads the log without a catalog call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    /// The table's vended credentials, when the caller holds them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
}

use serde_json::Value;

use super::json::{self, FromJson};
use super::name;
use super::Parent;

impl FromJson for Table {
    fn from_json(value: Value) -> Result<Self, String> {
        json::record(
            value,
            "table",
            "catalogs/…/tables/…",
            Self::named,
            |table| &table.name,
        )
    }
}

impl Table {
    fn named(name: &str) -> Option<Self> {
        if name::parse_table(name).is_some() {
            return Some(Self {
                name: name.to_owned(),
                ..Self::default()
            });
        }
        let (catalog, rest) = name.split_once('.')?;
        let (schema, table) = rest.rsplit_once('.')?;
        if catalog.is_empty() || schema.is_empty() || table.is_empty() {
            return None;
        }
        Some(Self {
            name: name::table(catalog, schema, table),
            ..Self::default()
        })
    }
}

impl FromJson for Partition {
    fn from_json(value: Value) -> Result<Self, String> {
        json::record(
            value,
            "partition",
            "catalogs/…/tables/…/partitions/…",
            Self::named,
            |partition| &partition.name,
        )
    }
}

impl Partition {
    fn named(name: &str) -> Option<Self> {
        if name::parse_partition(name).is_some() {
            return Some(Self {
                name: name.to_owned(),
                ..Self::default()
            });
        }
        let (catalog, rest) = name.split_once('.')?;
        let (rest, partition) = rest.rsplit_once('.')?;
        let (schema, table) = rest.rsplit_once('.')?;
        if catalog.is_empty() || schema.is_empty() || table.is_empty() || partition.is_empty() {
            return None;
        }
        Some(Self {
            name: name::partition(catalog, schema, table, partition),
            ..Self::default()
        })
    }
}

impl Parent for Table {
    const NOUN: &'static str = "table";

    fn name(&self) -> &str {
        &self.name
    }
}

impl Parent for Partition {
    const NOUN: &'static str = "partition";

    fn name(&self) -> &str {
        &self.name
    }
}
