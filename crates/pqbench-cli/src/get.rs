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
}

impl GetArgs {
    /// The command's name, used in configuration errors.
    pub(crate) fn name(&self) -> &'static str {
        match self.resource {
            GetResource::Metastore(_) => "get metastore",
            GetResource::Catalog(_) => "get catalog",
            GetResource::Schema(_) => "get schema",
            GetResource::Table(_) => "get table",
        }
    }
}

/// Read one resource: the walk's read verb.
pub(crate) async fn run(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &GetArgs,
) -> Result<(), CliError> {
    match &args.resource {
        GetResource::Metastore(args) => crate::metastore::get::run(workspace, args).await,
        GetResource::Catalog(args) => crate::catalog::get::run(workspace, args).await,
        GetResource::Schema(args) => crate::schema::get::run(workspace, args).await,
        GetResource::Table(args) => crate::table::get::run(workspace, lake, args).await,
    }
}
