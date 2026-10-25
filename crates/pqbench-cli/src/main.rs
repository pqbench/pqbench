//! `pqbench`: the command-line frontend.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bytemass;
mod catalog;
mod compression;
mod config;
mod credentials;
mod diff;
mod document;
mod emit;
mod error;
mod file;
mod format;
mod get;
mod help;
mod list;
mod lz;
mod metastore;
mod parse;
mod partition;
mod profile;
mod ratelimit;
mod render;
mod rows;
mod schema;
mod setup;
mod skill;
mod source;
mod stdin;
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
    /// The configuration every command shares (flags, with a `PQB_*` fallback).
    #[command(flatten)]
    config: config::ConfigArgs,
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
        about = help::GET_ABOUT,
        long_about = help::GET_LONG_ABOUT,
        after_help = help::GET_AFTER,
        after_long_help = help::GET_AFTER
    )]
    Get(get::GetArgs),
    #[command(about = "Render an NDJSON stream as tables")]
    Format(format::FormatArgs),
    #[command(
        about = help::LIST_ABOUT,
        long_about = help::LIST_LONG_ABOUT,
        after_help = help::LIST_AFTER,
        after_long_help = help::LIST_AFTER
    )]
    List(list::ListArgs),
    #[command(
        about = help::CREDENTIALS_ABOUT,
        long_about = help::CREDENTIALS_LONG_ABOUT,
        after_help = help::CREDENTIALS_AFTER,
        after_long_help = help::CREDENTIALS_AFTER
    )]
    Credentials(credentials::CredentialsArgs),
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
    match dispatch(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if e.downcast_ref::<error::Partial>().is_none() {
                eprintln!("error: {}", error::error_chain(e.as_ref()));
            }
            ExitCode::FAILURE
        }
    }
}

/// Resolve the shared configuration and context once, then run the command.
async fn dispatch(cli: Cli) -> Result<(), CliError> {
    let config = config::resolve(
        cli.command.name(),
        &cli.config,
        cli.command.requires_endpoint(),
    )?;
    let workspace = pqbench::Workspace::new(config)?;
    let lake = pqbench::Lake::new(&workspace);
    match cli.command {
        Command::Diff(args) => diff::run(&lake, &args).await,
        Command::Lz(args) => lz::run(&args).await,
        Command::Compression(args) => compression::run(&args).await,
        Command::Bytemass(args) => bytemass::run(&lake, &args).await,
        Command::Get(args) => get::run(&workspace, &lake, &args).await,
        Command::Format(args) => format::run(&args).await,
        Command::List(args) => list::run(&workspace, &lake, &args).await,
        Command::Credentials(args) => credentials::run(&workspace, &args).await,
        Command::RateLimit(args) => ratelimit::run(&args).await,
        Command::Profile(args) => profile::run(&args).await,
        Command::Skill(args) => skill::run(&args),
        Command::Viz(args) => viz::run(&args).await,
        Command::Setup(args) => setup::run(&workspace, &args),
    }
}

impl Command {
    /// The command's name, used in configuration errors.
    fn name(&self) -> &'static str {
        match self {
            Command::Diff(_) => "diff",
            Command::Lz(_) => "lz",
            Command::Compression(_) => "compression",
            Command::Bytemass(_) => "bytemass",
            Command::Get(args) => args.name(),
            Command::Format(_) => "format",
            Command::List(args) => args.name(),
            Command::Credentials(_) => "credentials",
            Command::RateLimit(_) => "ratelimit",
            Command::Profile(_) => "profile",
            Command::Skill(_) => "skill",
            Command::Viz(_) => "viz",
            Command::Setup(_) => "setup",
        }
    }

    /// Whether the command needs `PQB_ENDPOINT`. The catalog levels read a
    /// catalog; a storage-only command (`list partitions`, `list files`,
    /// `bytemass`, …) does not.
    fn requires_endpoint(&self) -> bool {
        match self {
            Command::Get(_) | Command::Credentials(_) => true,
            Command::List(args) => args.requires_endpoint(),
            _ => false,
        }
    }
}
