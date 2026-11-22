//! `get partition-price`: each partition's storage classes and cost.

use clap::Args;
use futures_util::stream;
use pqbench::v1;

use crate::source::{drive, flat_map};
use crate::{parse, CliError};

/// Arguments for `get partition-price`.
#[derive(Args)]
pub(crate) struct PriceArgs {
    /// partitions in flight at once
    #[arg(long, default_value_t = 16)]
    fan_out: usize,
}

/// Read each partition's price: `list partitions | get partition-price`.
///
/// `PartitionPrice` is the partition's singleton child (AIP-156), so the input
/// is its parent: Partition records on standard input, which carry the files
/// the price reads. Writes one PartitionPrice per partition, in input order
/// within the fan-out. Each partition costs up to three HEADs (its oldest,
/// sample and newest file) under its table's lease. A partition that fails is
/// reported and writes nothing; the others still do.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &PriceArgs,
) -> Result<(), CliError> {
    let parents = parse::parents::<v1::Partition>("get partition-price").await?;
    let lake = lake.clone();
    let daemon = daemon.clone();
    drive(flat_map(
        parents,
        args.fan_out,
        move |partition: v1::Partition| {
            let lake = lake.clone();
            let daemon = daemon.clone();
            async move {
                let reference = partition.name.clone();
                let table = v1::Partition::parse_name(&reference)
                    .map(|(catalog, schema, table, _)| v1::name::table(&catalog, &schema, &table))
                    .unwrap_or_default();
                let env = daemon.resolve_env(&table).await?;
                let credentials = v1::Credentials::from_env(&env);
                let lake = lake.with_lease(Some(&credentials));
                let price = pqbench::partition::price::price(&lake, &partition)
                    .await
                    .map_err(|error| crate::error::context(&reference, error))?;
                Ok(stream::iter([Ok::<_, CliError>(price)]))
            }
        },
    ))
    .await
}
