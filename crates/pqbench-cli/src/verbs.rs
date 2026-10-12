//! `get` / `ls`: the verb-first spellings of the walk's read commands.
//!
//! `pqbench get table X` and `pqbench table get X` are the same call: the verb
//! tree reuses each noun's leaf arguments, so the two spellings share one
//! parser and one implementation and cannot drift. The noun tree stays the
//! primary one (`pqbench <noun> --help` is the full page).

use clap::{Args, Subcommand};

use crate::CliError;

/// Arguments for the verb-first `get`.
#[derive(Args)]
pub(crate) struct GetArgs {
    #[command(subcommand)]
    command: GetCommand,
}

#[derive(Subcommand)]
enum GetCommand {
    /// Read the endpoint's metastore record
    Metastore(crate::metastore::OutputArgs),
    /// Read one catalog's record
    Catalog(crate::catalog::NameArgs),
    /// Read one schema's record (`CATALOG.SCHEMA`)
    Schema(crate::schema::NameArgs),
    /// Read one table's record (`CATALOG.SCHEMA.TABLE`)
    Table(crate::table::get::GetArgs),
}

/// Read one resource: the verb-first spelling of `pqbench <noun> get`.
pub(crate) async fn run_get(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &GetArgs,
) -> Result<(), CliError> {
    match &args.command {
        GetCommand::Metastore(args) => crate::metastore::get::run(workspace, args).await,
        GetCommand::Catalog(args) => crate::catalog::get::run(workspace, args).await,
        GetCommand::Schema(args) => crate::schema::get::run(workspace, args).await,
        GetCommand::Table(args) => crate::table::get::run(workspace, lake, args).await,
    }
}

/// Arguments for the verb-first `ls`.
#[derive(Args)]
pub(crate) struct LsArgs {
    #[command(subcommand)]
    command: LsCommand,
}

#[derive(Subcommand)]
enum LsCommand {
    /// List the catalogs at the endpoint
    Catalog(crate::catalog::ls::LsArgs),
    /// List the schemas in a catalog (`CATALOG`)
    Schema(crate::schema::ls::LsArgs),
    /// List the tables in a schema (`CATALOG.SCHEMA`)
    Table(crate::table::ls::LsArgs),
    /// List a table's natural partitions (`DIR`, or a Table stream on stdin)
    Partition(crate::partition::ls::LsArgs),
    /// List a partition's files (a Partition stream on stdin)
    File(crate::file::ls::LsArgs),
}

/// List a collection: the verb-first spelling of `pqbench <noun> ls`.
pub(crate) async fn run_ls(
    workspace: &pqbench::Workspace,
    lake: &pqbench::Lake,
    args: &LsArgs,
) -> Result<(), CliError> {
    match &args.command {
        LsCommand::Catalog(args) => crate::catalog::ls::run(workspace, args).await,
        LsCommand::Schema(args) => crate::schema::ls::run(workspace, args).await,
        LsCommand::Table(args) => crate::table::ls::run(workspace, args).await,
        LsCommand::Partition(args) => crate::partition::ls::run(lake, args).await,
        LsCommand::File(args) => crate::file::ls::run(lake, args).await,
    }
}

impl LsArgs {
    /// Whether the `ls` target reads a catalog: the storage-only levels
    /// (`partition ls`, `file ls`) do not.
    pub(crate) fn requires_endpoint(&self) -> bool {
        matches!(
            self.command,
            LsCommand::Catalog(_) | LsCommand::Schema(_) | LsCommand::Table(_)
        )
    }
}
