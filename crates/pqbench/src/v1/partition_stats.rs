use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::partition_version::State;

/// The totals of the partition versions under one catalog, schema or table,
/// its singleton child (AIP-156): `summarize partition-versions` folds them.
///
/// The partition totals count the live versions, the ones not `DELETED`: a
/// partition has at most one, so each counts one partition once.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PartitionStats {
    /// `catalogs/{catalog}/partitionStats`,
    /// `catalogs/{catalog}/schemas/{schema}/partitionStats`, or
    /// `catalogs/{catalog}/schemas/{schema}/tables/{table}/partitionStats`.
    pub name: String,
    /// The live versions: the partitions.
    pub partition_count: u64,
    /// The partitions' files and bytes, from their listings.
    pub file_count: u64,
    pub size_bytes: u64,
    /// The bytes removed from the logs but not vacuumed, where the logs
    /// report them.
    pub removed_size_bytes: u64,
    /// The partitions with a monthly cost: the ones the price job priced
    /// whose files have a storage class.
    pub priced_partition_count: u64,
    /// Those partitions' monthly cost, summed over their cheapest and their
    /// dearest class or access tier. Absent when none has a cost.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lowest_cost_per_month: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highest_cost_per_month: Option<f64>,
    /// When the newest file of any partition was written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modification_time: Option<u64>,
    /// Every version, `DELETED` ones included, counted by state.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub version_counts: BTreeMap<State, u64>,
    /// The versions whose last step failed.
    pub failed_version_count: u64,
}
