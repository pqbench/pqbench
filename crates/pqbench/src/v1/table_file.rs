use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::name;
use super::{Credentials, FileStats, Resource};

/// A table's reference to one [`Blob`](super::Blob): a row of the table's
/// log (the Delta `add`). The AIP-124 association between them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct TableFile {
    /// `…/tables/{table}/files/{file}`.
    #[resource(name = "catalogs/{catalog}/schemas/{schema}/tables/{table}/files/{file}")]
    pub name: String,
    /// The blob holding the bytes, a [`Blob`](super::Blob) name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    /// The path as the log records it, relative to the table's storage root.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    /// The size the log claims, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// When the log says the file was created, milliseconds since the Unix
    /// epoch. Delta records it on every `add`; Iceberg's manifests carry no
    /// per-file time, so it is absent there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modification_time: Option<u64>,
    /// Partition values from the log; a value is `None` for a null partition.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_values: BTreeMap<String, Option<String>>,
    /// The log's statistics, when it carries them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<FileStats>,
    /// The vended storage credentials to read [`Self::blob`] with, when the
    /// caller holds them. Held on the record so the blob read uses the lease
    /// the log read used.
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
