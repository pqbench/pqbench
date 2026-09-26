use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bytemass;
mod compression;
mod document;
mod dump;
mod emit;
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
    about = "lzbench for parquet",
    after_help = r#"
Examples:
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench bytemass data.parquet
  pqbench bytemass part-1.parquet part-2.parquet
  pqbench bytemass 'data/*.parquet'
  pqbench table ./delta-table -o table.ndjson.zst
  pqbench table ./delta-table | pqbench bytemass
  pqbench table ./iceberg-table | pqbench bytemass
  pqbench lake ./warehouse | pqbench table | pqbench bytemass
  pqbench lake s3://bucket/warehouse | pqbench table | pqbench bytemass
  pqbench table ./delta-table | jq -c 'select(.kind!="pqbench.table-file" or (.path|startswith("year=2024/")))' | pqbench dump ./sample
  pqbench bytemass data.parquet | pqbench viz -o report && xdg-open report.html
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
  pqbench table ./delta-table | jq -c 'select(.kind!="pqbench.table-file" or (.path|startswith("year=2024/")))' | pqbench bytemass
  pqbench lake ./warehouse | pqbench table | pqbench bytemass
  pqbench bytemass table.ndjson.zst
  pqbench bytemass data.parquet | pqbench viz -o report && xdg-open report.html
"#)]
    Bytemass(bytemass::BytemassArgs),
    /// fetch table metadata (detect the format, then load the log)
    #[command(after_help = r#"Examples:
  pqbench table ./delta-table -o table.ndjson.zst
  pqbench table ./delta-table | pqbench bytemass
  pqbench table ./delta-table -o table.ndjson.zst | pqbench bytemass
  producer | pqbench table | pqbench bytemass
"#)]
    Table(table::TableArgs),
    /// list the tables in a lake
    #[command(after_help = r#"Examples:
  pqbench lake ./warehouse
  pqbench lake s3://bucket/warehouse --max-depth 2
  pqbench lake ./warehouse --include 'sales/*' --exclude 'sales/tmp*'
  pqbench lake creds.json --include 'main.default.*' --exclude 'main.default.tmp*'
  pqbench lake creds.json | pqbench table | pqbench bytemass
"#)]
    Lake(lake::LakeArgs),
    /// copy the Parquet files a table names to a local directory
    #[command(after_help = r#"Examples:
  pqbench table ./delta-table | pqbench dump ./sample
  pqbench table s3://bucket/table | pqbench dump ./sample
  pqbench dump ./sample s3://bucket/table
  pqbench lake ./warehouse | pqbench table | pqbench dump ./mirror
"#)]
    Dump(dump::DumpArgs),
    /// collect a bytemass stream into a static HTML treemap
    #[command(after_help = r#"Examples:
  pqbench bytemass data.parquet | pqbench viz -o report
  pqbench table ./delta-table | pqbench bytemass | pqbench viz -o report
  pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report
  xdg-open report.html
"#)]
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
