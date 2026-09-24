use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod bench;
mod bytemass;
mod compression;
mod document;
mod emit;
mod lz;
mod table;

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
  pqbench bytemass table.ndjson.zst
  pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
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
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Lz(args) => lz::run(&args),
        Command::Compression(args) => compression::run(&args),
        Command::Bytemass(args) => bytemass::run(&args).await,
        Command::Table(args) => table::run(&args).await,
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
