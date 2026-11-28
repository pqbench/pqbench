use serde::{Deserialize, Serialize};

use super::{Resource, StorageTierCost};

/// A partition's storage price at one etag, the partition's singleton child
/// (AIP-156): the storage classes of the files `get partition-price` looked
/// at, and what the partition's bytes cost in each.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct PartitionPrice {
    /// `…/tables/{table}/partitions/{partition}/price`.
    #[resource(
        name = "catalogs/{catalog}/schemas/{schema}/tables/{table}/partitions/{partition}/price"
    )]
    pub name: String,
    /// The partition's etag when it was priced.
    pub partition_etag: String,
    /// The storage classes of the partition's oldest, sample and newest
    /// files, without repeats, in that order. Lifecycle moves objects by age,
    /// so one class covers the partition and two mean a transition inside it.
    /// Empty when the files are not S3 objects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_classes: Vec<String>,
    /// The partition's bytes priced in each class it shows: one entry per
    /// class, and one per access sub-tier for a class that prices by access.
    /// With one class this is the cost; with two they bound it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub storage_costs: Vec<StorageTierCost>,
    /// The partition's removed bytes priced the same way: what a vacuum
    /// would stop paying.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_storage_costs: Vec<StorageTierCost>,
    /// Whether any of the sampled files needs a restore before it can be read.
    /// Absent when no sampled file is an S3 object. A file whose restore has
    /// completed is not restore-required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_required: Option<bool>,
}
