//! `partition::stats`: partition versions folded into totals per catalog,
//! schema and table.

use std::collections::BTreeMap;

use crate::v1::{PartitionStats, PartitionVersion};

/// The running totals: one [`PartitionStats`] per catalog, schema and table
/// seen, so the memory is the number of those, not of versions.
#[derive(Debug, Default)]
pub struct Summary {
    stats: BTreeMap<String, PartitionStats>,
}

impl Summary {
    /// Count one version into its catalog's, schema's and table's totals.
    ///
    /// # Errors
    /// Fails when the version's name is not a partition version's.
    pub fn add(&mut self, version: &PartitionVersion) -> Result<(), String> {
        let ids = PartitionVersion::parse_name(&version.name)
            .ok_or_else(|| format!("{} is not a partition version", version.name))?;
        let (catalog, schema, table, _, _) = ids;
        let parents = [
            format!("catalogs/{catalog}"),
            format!("catalogs/{catalog}/schemas/{schema}"),
            format!("catalogs/{catalog}/schemas/{schema}/tables/{table}"),
        ];
        for parent in parents {
            let stats = self
                .stats
                .entry(parent)
                .or_insert_with_key(|parent| PartitionStats {
                    name: format!("{parent}/partitionStats"),
                    ..PartitionStats::default()
                });
            add(stats, version);
        }
        Ok(())
    }

    /// The totals, in name order: each catalog before its schemas, each
    /// schema before its tables.
    #[must_use]
    pub fn into_stats(self) -> Vec<PartitionStats> {
        self.stats.into_values().collect()
    }
}

fn add(stats: &mut PartitionStats, version: &PartitionVersion) {
    *stats.version_counts.entry(version.state).or_default() += 1;
    if version.failure_count > 0 {
        stats.failed_version_count += 1;
    }
    stats.partition_count += 1;
    stats.file_count += version.file_count;
    stats.size_bytes += version.size_bytes;
    stats.removed_size_bytes += version.removed_size_bytes.unwrap_or(0);
    if let (Some(lowest), Some(highest)) = (
        version.lowest_cost_per_month,
        version.highest_cost_per_month,
    ) {
        stats.priced_partition_count += 1;
        *stats.lowest_cost_per_month.get_or_insert(0.0) += lowest;
        *stats.highest_cost_per_month.get_or_insert(0.0) += highest;
    }
    stats.last_modification_time = stats
        .last_modification_time
        .max(version.last_modification_time);
}
