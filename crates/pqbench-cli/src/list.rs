//! `list`: list a collection of the walk.

use clap::{Args, Subcommand};

use crate::CliError;

/// Arguments for `list`.
#[derive(Args)]
pub(crate) struct ListArgs {
    #[command(subcommand)]
    collection: ListCollection,
}

/// The collection a `list` enumerates.
#[derive(Subcommand)]
enum ListCollection {
    /// List the catalogs at the endpoint
    Catalogs(crate::catalog::list::ListArgs),
    /// List the schemas in a catalog (`CATALOG`)
    Schemas(crate::schema::list::ListArgs),
    /// List the tables in a schema (`CATALOG.SCHEMA`)
    Tables(crate::table::list::ListArgs),
    /// List a table's natural partitions (`DIR`, or a `Table` stream on stdin)
    Partitions(crate::partition::list::ListArgs),
    /// List a partition's files (a `Partition` stream on stdin)
    Files(crate::file::list::ListArgs),
}

/// List a collection: the walk's list verb.
pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &ListArgs,
) -> Result<(), CliError> {
    match &args.collection {
        ListCollection::Catalogs(args) => crate::catalog::list::run(workspace, args).await,
        ListCollection::Schemas(args) => crate::schema::list::run(workspace, args).await,
        ListCollection::Tables(args) => crate::table::list::run(workspace, args).await,
        ListCollection::Partitions(args) => crate::partition::list::run(lake, args).await,
        ListCollection::Files(args) => crate::file::list::run(lake, args).await,
    }
}

impl ListArgs {
    /// The command's name, used in configuration errors.
    pub(crate) fn name(&self) -> &'static str {
        match self.collection {
            ListCollection::Catalogs(_) => "list catalogs",
            ListCollection::Schemas(_) => "list schemas",
            ListCollection::Tables(_) => "list tables",
            ListCollection::Partitions(_) => "list partitions",
            ListCollection::Files(_) => "list files",
        }
    }

    /// Whether the collection reads a catalog: the storage-only levels
    /// (`list partitions`, `list files`) do not.
    pub(crate) fn requires_endpoint(&self) -> bool {
        matches!(
            self.collection,
            ListCollection::Catalogs(_) | ListCollection::Schemas(_) | ListCollection::Tables(_)
        )
    }
}
