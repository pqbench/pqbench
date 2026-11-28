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
    /// List a table's active files (Table refs on stdin)
    Files(crate::file::list::ListArgs),
    /// List a table's partitions (Table refs on stdin)
    Partitions(crate::partition::list::ListArgs),
    /// List the driver's partition versions (`PARENT`, or all)
    PartitionVersions(crate::partition::version::ListArgs),
    /// List a file's columns and their byte masses (Table-file refs on stdin)
    Columns(crate::bytemass::BytemassArgs),
    /// List potential storage suggestions for each local blob (PATHs, or Table-file refs on stdin)
    Suggestions(crate::lint::SuggestionsArgs),
}

/// List a collection: the walk's list verb.
pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    daemon: &crate::daemon::Daemon,
    args: &ListArgs,
) -> Result<(), CliError> {
    match &args.collection {
        ListCollection::Catalogs(args) => crate::catalog::list::run(workspace, args).await,
        ListCollection::Schemas(args) => crate::schema::list::run(workspace, args).await,
        ListCollection::Tables(args) => crate::table::list::run(workspace, args).await,
        ListCollection::Files(args) => crate::file::list::run(lake, daemon, args).await,
        ListCollection::Partitions(args) => crate::partition::list::run(lake, daemon, args).await,
        ListCollection::PartitionVersions(args) => crate::partition::version::run(args).await,
        ListCollection::Columns(args) => crate::bytemass::run(lake, daemon, args).await,
        ListCollection::Suggestions(args) => crate::lint::run(args).await,
    }
}

impl ListArgs {
    /// The command's name, used in configuration errors.
    pub(crate) fn name(&self) -> &'static str {
        match self.collection {
            ListCollection::Catalogs(_) => "list catalogs",
            ListCollection::Schemas(_) => "list schemas",
            ListCollection::Tables(_) => "list tables",
            ListCollection::Files(_) => "list files",
            ListCollection::Partitions(_) => "list partitions",
            ListCollection::PartitionVersions(_) => "list partition-versions",
            ListCollection::Columns(_) => "list columns",
            ListCollection::Suggestions(_) => "list suggestions",
        }
    }

    /// Whether the collection reads a catalog: the storage-only level
    /// (`list files`) does not.
    pub(crate) fn requires_endpoint(&self) -> bool {
        matches!(
            self.collection,
            ListCollection::Catalogs(_) | ListCollection::Schemas(_) | ListCollection::Tables(_)
        )
    }
}
