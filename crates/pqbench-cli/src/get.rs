//! `get`: read one resource of the walk.

use clap::{Args, Subcommand};

use crate::CliError;

/// Arguments for `get`.
#[derive(Args)]
pub(crate) struct GetArgs {
    #[command(subcommand)]
    resource: GetResource,
}

/// The resource a `get` reads.
#[derive(Subcommand)]
enum GetResource {
    /// Read the endpoint's metastore record
    Metastore(crate::metastore::OutputArgs),
    /// Read one catalog's record
    Catalog(crate::catalog::NameArgs),
    /// Read one schema's record (`CATALOG.SCHEMA`)
    Schema(crate::schema::NameArgs),
    /// Read one table's record (`CATALOG.SCHEMA.TABLE`)
    Table(crate::table::get::GetArgs),
    /// Read one version's stored results (`name`, or a partition for its newest live version)
    PartitionVersion(crate::partition::version::GetArgs),
    /// Read each partition's storage classes and cost (Partition records on stdin)
    #[command(long_about = crate::help::GET_PARTITION_PRICE_LONG_ABOUT)]
    PartitionPrice(crate::partition::price::PriceArgs),
    /// Verify one column's potential suggestion (Suggestion refs on stdin)
    Suggestion(crate::lint::VerifyArgs),
}

impl GetArgs {
    /// The command's name, used in configuration errors.
    pub(crate) fn name(&self) -> &'static str {
        match self.resource {
            GetResource::Metastore(_) => "get metastore",
            GetResource::Catalog(_) => "get catalog",
            GetResource::Schema(_) => "get schema",
            GetResource::Table(_) => "get table",
            GetResource::PartitionVersion(_) => "get partition-version",
            GetResource::PartitionPrice(_) => "get partition-price",
            GetResource::Suggestion(_) => "get suggestion",
        }
    }

    /// Whether the resource needs `PQB_ENDPOINT`. Suggestions measure a local
    /// file, like `list suggestions`; a partition's price HEADs its files.
    pub(crate) fn requires_endpoint(&self) -> bool {
        !matches!(
            &self.resource,
            GetResource::Suggestion(_)
                | GetResource::PartitionPrice(_)
                | GetResource::PartitionVersion(_)
        )
    }
}

/// Read one resource: the walk's read verb.
pub(crate) async fn run(
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &GetArgs,
) -> Result<(), CliError> {
    match &args.resource {
        GetResource::Metastore(args) => crate::metastore::get::run(daemon, args).await,
        GetResource::Catalog(args) => crate::catalog::get::run(daemon, args).await,
        GetResource::Schema(args) => crate::schema::get::run(daemon, args).await,
        GetResource::Table(args) => crate::table::get::run(daemon, args).await,
        GetResource::PartitionVersion(args) => crate::partition::version::get(args).await,
        GetResource::PartitionPrice(args) => crate::partition::price::run(lake, daemon, args).await,
        GetResource::Suggestion(args) => crate::lint::verify(args).await,
    }
}
