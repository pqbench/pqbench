//! `partition::price`: a partition's storage classes and what its bytes cost.

use std::fmt;

use futures_util::future::try_join_all;

use crate::price::{object_storage, storage_prices, GIBIBYTE};
use crate::v1;
use crate::Lake;

/// Errors pricing a partition.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A file's storage could not be read.
    Blob {
        /// The blob name.
        name: String,
        /// The read failure.
        source: crate::blob::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Blob { name, .. } => write!(f, "cannot read the storage class of {name}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Blob { source, .. } => Some(source),
        }
    }
}

/// Price a partition: one HEAD for each of its oldest, sample and newest
/// files that is an S3 object (S3 omits the class for `STANDARD`), then its
/// bytes and removed bytes at each class's list price. Nothing is read from
/// the files, so the HEADs do not count as Intelligent-Tiering access.
///
/// The caller scopes `lake` with the table's lease (`Lake::with_lease`).
///
/// # Errors
/// Fails when a HEAD fails.
pub async fn price(lake: &Lake, partition: &v1::Partition) -> Result<v1::PartitionPrice, Error> {
    let mut blobs: Vec<&str> = Vec::new();
    for file in [
        &partition.first_file,
        &partition.sample_file,
        &partition.last_file,
    ] {
        blobs.extend(
            file.as_ref()
                .and_then(|file| file.blob.as_deref())
                .filter(|blob| object_storage_blob(blob) && !blobs.contains(blob)),
        );
    }
    let heads = try_join_all(blobs.into_iter().map(|name| async move {
        crate::blob::get(lake, name)
            .await
            .map_err(|source| Error::Blob {
                name: name.to_string(),
                source,
            })
    }))
    .await?;
    let mut storage_classes: Vec<String> = Vec::new();
    let mut restore_required = false;
    for blob in &heads {
        restore_required |= blob.restore_required == Some(true);
        let class = blob
            .storage_class
            .clone()
            .unwrap_or_else(|| "STANDARD".to_string());
        if !storage_classes.contains(&class) {
            storage_classes.push(class);
        }
    }
    Ok(v1::PartitionPrice {
        name: format!("{}/price", partition.name),
        partition_etag: partition.etag.clone(),
        storage_costs: costs(&storage_classes, partition.size_bytes),
        removed_storage_costs: partition
            .removed_size_bytes
            .filter(|bytes| *bytes > 0)
            .map(|bytes| costs(&storage_classes, bytes))
            .unwrap_or_default(),
        storage_classes,
        restore_required: (!heads.is_empty()).then_some(restore_required),
    })
}

fn object_storage_blob(blob: &str) -> bool {
    v1::name::parse_blob(blob).is_some_and(|(service, _)| object_storage(&service))
}

/// `size_bytes` at each class's list prices; a class without a price adds
/// nothing, and no bytes cost nothing at every class that has a price.
fn costs(storage_classes: &[String], size_bytes: u64) -> Vec<v1::StorageTierCost> {
    let gibibytes = size_bytes as f64 / GIBIBYTE;
    storage_classes
        .iter()
        .filter_map(|class| Some((class, storage_prices(class)?)))
        .flat_map(|(class, prices)| {
            prices.iter().map(move |(tier, price)| v1::StorageTierCost {
                storage_class: class.clone(),
                access_tier: tier.map(str::to_string),
                dollars_per_gibibyte_per_month: *price,
                cost_per_month: gibibytes * price,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::costs;

    /// A partition with no bytes has a price too: zero at every class, so it
    /// counts as priced rather than unknown.
    #[test]
    fn no_bytes_cost_nothing_at_every_class() {
        let classes = ["STANDARD".to_string(), "GLACIER".to_string()];
        let costs = costs(&classes, 0);
        assert!(!costs.is_empty());
        for cost in &costs {
            assert_eq!(cost.cost_per_month, 0.0, "{cost:?}");
        }
        assert_eq!(
            costs
                .iter()
                .map(|cost| cost.storage_class.as_str())
                .collect::<Vec<_>>(),
            ["STANDARD", "GLACIER"]
        );
    }

    /// Bytes are priced by the gibibyte at each class's list price.
    #[test]
    fn bytes_are_priced_at_each_class() {
        let costs = costs(&["STANDARD".to_string()], 1024 * 1024 * 1024);
        assert_eq!(costs.len(), 1);
        assert!(costs[0].cost_per_month > 0.0, "{costs:?}");
        assert_eq!(
            costs[0].cost_per_month,
            costs[0].dollars_per_gibibyte_per_month
        );
    }
}
