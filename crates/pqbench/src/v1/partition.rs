use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{Resource, TableFile};

/// One partition of a table's snapshot: the active files that share its
/// partition values, folded into totals. An unpartitioned table has one.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct Partition {
    /// `…/tables/{table}/partitions/{partition}`. `{partition}` is the
    /// Hive-style path of the values (`k=v/k2=v2`, the table's partition
    /// column order), encoded to one segment; `unpartitioned` for a table
    /// without partition columns, and for removed files whose `remove`
    /// action records no partition values.
    #[resource(name = "catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions/{partition}")]
    pub name: String,
    /// The log's partition values; a value is `None` for a null partition.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub partition_values: BTreeMap<String, Option<String>>,
    /// Changes when a file is added to or removed from the partition: a hash
    /// of each file's path and size, combined independently of order.
    pub etag: String,
    /// Active files in the partition.
    pub file_count: u64,
    /// Sum of the files' sizes, as the log claims them.
    pub size_bytes: u64,
    /// Sum of the files' record counts, when every file has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_count: Option<u64>,
    /// The smallest file's size.
    pub smallest_file_size_bytes: u64,
    /// The largest file's size.
    pub largest_file_size_bytes: u64,
    /// The oldest file by the log's modification time. Absent when the log
    /// records no per-file time (Iceberg).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_file: Option<TableFile>,
    /// The newest file by the log's modification time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_file: Option<TableFile>,
    /// The file a measurement reads for the partition: the one whose path
    /// and size hash lowest, so it is the same file while the partition
    /// holds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_file: Option<TableFile>,
    /// Files the snapshot removed whose tombstones it still keeps and no
    /// active file holds: stored, and billed, until a vacuum deletes them.
    /// Absent when the format's removed files are not read (Iceberg).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_file_count: Option<u64>,
    /// Sum of the removed files' sizes, as the `remove` actions record them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub removed_size_bytes: Option<u64>,
}
