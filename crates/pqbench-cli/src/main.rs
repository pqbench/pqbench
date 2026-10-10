use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bytemass;
mod catalog;
mod compression;
mod credentials;
mod diff;
mod document;
mod emit;
mod experiment;
mod help;
mod lz;
mod metastore;
mod partition;
mod profile;
mod ratelimit;
mod schema;
mod setup;
mod skill;
mod source;
mod table;
mod tablev2;
mod viz;

/// The CLI's single error channel: any error from the io, parquet, or codec
/// layers, converted via `?`.
pub(crate) type CliError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "pqbench",
    about = help::ROOT_ABOUT,
    long_about = help::ROOT_LONG_ABOUT,
    after_help = help::ROOT_AFTER,
    after_long_help = help::ROOT_AFTER
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Diff(diff::DiffArgs),
    #[command(
        about = help::LZ_ABOUT,
        long_about = help::LZ_LONG_ABOUT,
        after_help = help::LZ_AFTER,
        after_long_help = help::LZ_AFTER
    )]
    Lz(bench::BenchArgs),
    #[command(
        about = help::COMPRESSION_ABOUT,
        long_about = help::COMPRESSION_LONG_ABOUT,
        after_help = help::COMPRESSION_AFTER,
        after_long_help = help::COMPRESSION_AFTER
    )]
    Compression(compression::CompressionArgs),
    #[command(
        about = help::BYTEMASS_ABOUT,
        long_about = help::BYTEMASS_LONG_ABOUT,
        after_help = help::BYTEMASS_AFTER,
        after_long_help = help::BYTEMASS_AFTER
    )]
    Bytemass(bytemass::BytemassArgs),
    #[command(
        about = help::TABLE_ABOUT,
        long_about = help::TABLE_LONG_ABOUT,
        after_help = help::TABLE_AFTER,
        after_long_help = help::TABLE_AFTER
    )]
    Table(table::TableArgs),
    #[command(
        about = help::METASTORE_ABOUT,
        long_about = help::METASTORE_LONG_ABOUT,
        after_help = help::METASTORE_AFTER,
        after_long_help = help::METASTORE_AFTER
    )]
    Metastore(metastore::MetastoreArgs),
    #[command(
        about = help::CATALOG_ABOUT,
        long_about = help::CATALOG_LONG_ABOUT,
        after_help = help::CATALOG_AFTER,
        after_long_help = help::CATALOG_AFTER
    )]
    Catalog(catalog::CatalogArgs),
    #[command(
        about = help::SCHEMA_ABOUT,
        long_about = help::SCHEMA_LONG_ABOUT,
        after_help = help::SCHEMA_AFTER,
        after_long_help = help::SCHEMA_AFTER
    )]
    Schema(schema::SchemaArgs),
    #[command(
        name = "tablev2",
        about = help::TABLEV2_ABOUT,
        long_about = help::TABLEV2_LONG_ABOUT,
        after_help = help::TABLEV2_AFTER,
        after_long_help = help::TABLEV2_AFTER
    )]
    TableV2(tablev2::TableV2Args),
    #[command(
        about = help::CREDENTIALS_ABOUT,
        long_about = help::CREDENTIALS_LONG_ABOUT,
        after_help = help::CREDENTIALS_AFTER,
        after_long_help = help::CREDENTIALS_AFTER
    )]
    Credentials(credentials::CredentialsArgs),
    #[command(
        about = help::PARTITION_ABOUT,
        long_about = help::PARTITION_LONG_ABOUT,
        after_help = help::PARTITION_AFTER,
        after_long_help = help::PARTITION_AFTER
    )]
    Partition(partition::PartitionArgs),
    #[command(
        name = "ratelimit",
        about = help::RATELIMIT_ABOUT,
        long_about = help::RATELIMIT_LONG_ABOUT,
        after_help = help::RATELIMIT_AFTER,
        after_long_help = help::RATELIMIT_AFTER
    )]
    RateLimit(ratelimit::RateLimitArgs),
    #[command(
        about = help::PROFILE_ABOUT,
        long_about = help::PROFILE_LONG_ABOUT,
        after_help = help::PROFILE_AFTER,
        after_long_help = help::PROFILE_AFTER
    )]
    Profile(profile::ProfileArgs),
    #[command(
        about = help::EXPERIMENT_ABOUT,
        long_about = help::EXPERIMENT_LONG_ABOUT,
        after_help = help::EXPERIMENT_AFTER,
        after_long_help = help::EXPERIMENT_AFTER
    )]
    Experiment(experiment::ExperimentArgs),
    #[command(
        about = help::SKILL_ABOUT,
        long_about = help::SKILL_LONG_ABOUT,
        after_help = help::SKILL_AFTER,
        after_long_help = help::SKILL_AFTER
    )]
    Skill(skill::SkillArgs),
    #[command(
        about = help::VIZ_ABOUT,
        long_about = help::VIZ_LONG_ABOUT,
        after_help = help::VIZ_AFTER,
        after_long_help = help::VIZ_AFTER
    )]
    Viz(viz::VizArgs),
    #[command(
        about = help::SETUP_ABOUT,
        long_about = help::SETUP_LONG_ABOUT,
        after_help = help::SETUP_AFTER,
        after_long_help = help::SETUP_AFTER
    )]
    Setup(setup::SetupArgs),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    // pqbench reads tables from outside Databricks with explicit credentials
    // (vended, or on the lake source); it never relies on EC2 instance
    // metadata. The AWS SDK's config chain probes IMDS for the region
    // otherwise, and hangs where that endpoint is blackholed. Default the
    // probe off before any command builds an AWS client; a caller that sets
    // AWS_EC2_METADATA_DISABLED wins.
    if std::env::var_os("AWS_EC2_METADATA_DISABLED").is_none() {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Diff(args) => diff::run(&args).await,
        Command::Lz(args) => lz::run(&args).await,
        Command::Compression(args) => compression::run(&args).await,
        Command::Bytemass(args) => bytemass::run(&args).await,
        Command::Table(args) => table::run(&args).await,
        Command::Metastore(args) => metastore::run(&args).await,
        Command::Catalog(args) => catalog::run(&args).await,
        Command::Schema(args) => schema::run(&args).await,
        Command::TableV2(args) => tablev2::run(&args).await,
        Command::Credentials(args) => credentials::run(&args).await,
        Command::Partition(args) => partition::run(&args).await,
        Command::RateLimit(args) => ratelimit::run(&args).await,
        Command::Profile(args) => profile::run(&args).await,
        Command::Experiment(args) => experiment::run(&args).await,
        Command::Skill(args) => skill::run(&args),
        Command::Viz(args) => viz::run(&args).await,
        Command::Setup(args) => setup::run(&args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
