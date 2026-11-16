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
            GetResource::Suggestion(_) => "get suggestion",
        }
    }

    /// Whether the resource needs `PQB_ENDPOINT`. Suggestions measure a local
    /// file, like `list suggestions`.
    pub(crate) fn requires_endpoint(&self) -> bool {
        !matches!(&self.resource, GetResource::Suggestion(_))
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
        GetResource::Suggestion(args) => crate::lint::verify(lake, args).await,
    }
}
