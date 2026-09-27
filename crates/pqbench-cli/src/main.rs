use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bytemass;
mod catalog;
mod compression;
mod document;
mod dump;
mod emit;
mod experiment;
mod help;
mod lake;
mod lz;
mod metastore;
mod profile;
mod ratelimit;
mod schema;
mod skill;
mod source;
mod table;
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
        about = help::LAKE_ABOUT,
        long_about = help::LAKE_LONG_ABOUT,
        after_help = help::LAKE_AFTER,
        after_long_help = help::LAKE_AFTER
    )]
    Lake(lake::LakeArgs),
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
        name = "ratelimit",
        about = help::RATELIMIT_ABOUT,
        long_about = help::RATELIMIT_LONG_ABOUT,
        after_help = help::RATELIMIT_AFTER,
        after_long_help = help::RATELIMIT_AFTER
    )]
    RateLimit(ratelimit::RateLimitArgs),
    #[command(
        about = help::DUMP_ABOUT,
        long_about = help::DUMP_LONG_ABOUT,
        after_help = help::DUMP_AFTER,
        after_long_help = help::DUMP_AFTER
    )]
    Dump(dump::DumpArgs),
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
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Lz(args) => lz::run(&args).await,
        Command::Compression(args) => compression::run(&args).await,
        Command::Bytemass(args) => bytemass::run(&args).await,
        Command::Table(args) => table::run(&args).await,
        Command::Lake(args) => lake::run(&args).await,
        Command::Metastore(args) => metastore::run(&args).await,
        Command::Catalog(args) => catalog::run(&args).await,
        Command::Schema(args) => schema::run(&args).await,
        Command::RateLimit(args) => ratelimit::run(&args).await,
        Command::Dump(args) => dump::run(&args).await,
        Command::Profile(args) => profile::run(&args).await,
        Command::Experiment(args) => experiment::run(&args).await,
        Command::Skill(args) => skill::run(&args),
        Command::Viz(args) => viz::run(&args).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
