use super::name;
use super::{Credentials, FileStats};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A storage blob: the bytes a data file lives in — a local file or an S3
/// object, named by its service.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Blob {
    /// `blobs/{service}/{locator}`.
    pub name: String,
    /// The object's size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// The storage class or tier (`STANDARD`, `STANDARD_IA`, …), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Row groups in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_count: Option<u64>,
    /// Leaf columns in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_count: Option<u64>,
}

/// A table's reference to one [`Blob`]: a row of the table's log (the Delta
/// `add`). The AIP-124 association between them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TableFile {
    /// `…/tables/{table}/files/{file}`.
    pub name: String,
    /// The blob holding the bytes, a [`Blob`] name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    /// The table's storage root, denormalized onto the file so a reader reads
    /// [`Self::blob`] without the table record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_root: Option<String>,
    /// The path as the log records it, relative to [`Self::storage_root`].
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    /// The size the log claims, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Partition values from the log; a value is `None` for a null partition.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_values: BTreeMap<String, Option<String>>,
    /// The log's statistics, when it carries them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<FileStats>,
    /// The vended storage credentials to read [`Self::blob`] with, when the
    /// caller holds them. A sibling to the resource on the wire, like the
    /// lease on a [`Table`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,
}

impl TableFile {
    /// The storage URI the bytes are at, from [`Self::blob`]. `None` when the
    /// record carries no blob.
    #[must_use]
    pub fn uri(&self) -> Option<String> {
        self.blob.as_deref().and_then(name::blob_uri)
    }
}

/// A file's copy of a declared column: the physical unit the analyses read.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Column {
    /// `…/blobs/{service}/{locator}/columns/{column}`.
    pub name: String,
    /// The declared column this file column is a copy of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_column: Option<String>,
    /// The column's position in the file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    /// The physical type of the leaf column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_data_type: Option<String>,
    /// Row groups the column spans.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_group_count: Option<u64>,
}
