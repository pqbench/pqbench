use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bytemass;
mod catalog;
mod compression;
mod document;
mod dump;
mod emit;
mod filter;
mod iceberg;
mod lake;
mod lz;
mod table;
mod unity;

/// The CLI's single error channel: any error from the io, parquet, or codec
/// layers, converted via `?`.
pub(crate) type CliError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "pqbench",
    about = "lzbench for parquet",
    after_help = r#"
Examples:
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench bytemass data.parquet
  pqbench bytemass part-1.parquet part-2.parquet
  pqbench bytemass 'data/*.parquet'
  pqbench table ./delta-table -o table.ndjson.zst
  pqbench table ./iceberg-table | pqbench bytemass
  pqbench lake ./warehouse | pqbench table | pqbench bytemass
  pqbench lake s3://bucket/warehouse | pqbench table | pqbench bytemass
  pqbench table ./delta-table | pqbench dump --row-groups first:1 -o sample.parquet
  pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
"#
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// lzbench-style compression benchmark over raw file bytes
    Lz(bench::BenchArgs),
    /// lzbench-style codec sweep over encoded parquet pages (NONE-compressed input)
    Compression(compression::CompressionArgs),
    /// export per-column byte masses (on-disk bytes/row)
    #[command(after_help = r#"
Examples:
  pqbench bytemass data.parquet
  pqbench table ./delta-table | pqbench bytemass
  pqbench table ./delta-table | pqbench bytemass --include 'year=2024/**' --sample first:10
  pqbench lake ./warehouse | pqbench table | pqbench bytemass
  pqbench bytemass table.ndjson.zst
  pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
"#)]
    Bytemass(bytemass::BytemassArgs),
    /// fetch table metadata (detect the format, then load the log)
    #[command(after_help = r#"Examples:
  pqbench table ./delta-table -o table.ndjson.zst
  pqbench table ./delta-table --concurrency 8 | pqbench bytemass --concurrency 8
  pqbench table ./delta-table -o table.ndjson.zst | pqbench bytemass
  producer | pqbench table | pqbench bytemass
"#)]
    Table(table::TableArgs),
    /// list the tables in a lake
    #[command(after_help = r#"Examples:
  pqbench lake ./warehouse
  pqbench lake s3://bucket/warehouse --max-depth 2 --concurrency 8
  pqbench lake ./warehouse --include 'sales/*' --exclude 'sales/tmp*'
  pqbench lake creds.json --include 'main.default.*' --exclude 'main.default.tmp*'
  pqbench lake creds.json --concurrency 8 | pqbench table | pqbench bytemass
"#)]
    Lake(lake::LakeArgs),
    /// write a row sample from parquet files or a table document
    #[command(after_help = r#"Examples:
  pqbench dump data.parquet -o sample.parquet
  pqbench table ./delta-table | pqbench dump --row-groups first:1 -o sample.parquet
  pqbench table ./delta-table | pqbench dump --include 'year=2024/**' --sample first:1 -o sample.parquet
  pqbench lake ./warehouse | pqbench table | pqbench dump -o sample.parquet
"#)]
    Dump(dump::DumpArgs),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Lz(args) => lz::run(&args),
        Command::Compression(args) => compression::run(&args),
        Command::Bytemass(args) => bytemass::run(&args),
        Command::Table(args) => table::run(&args),
        Command::Lake(args) => lake::run(&args),
        Command::Dump(args) => dump::run(&args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
