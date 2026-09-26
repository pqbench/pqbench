use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bytemass;
mod compression;
mod document;
mod dump;
mod emit;
mod help;
mod lake;
mod lz;
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
        about = help::DUMP_ABOUT,
        long_about = help::DUMP_LONG_ABOUT,
        after_help = help::DUMP_AFTER,
        after_long_help = help::DUMP_AFTER
    )]
    Dump(dump::DumpArgs),
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
        Command::Lz(args) => lz::run(&args),
        Command::Compression(args) => compression::run(&args),
        Command::Bytemass(args) => bytemass::run(&args).await,
        Command::Table(args) => table::run(&args).await,
        Command::Lake(args) => lake::run(&args).await,
        Command::Dump(args) => dump::run(&args).await,
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
