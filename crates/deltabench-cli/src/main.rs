use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};

#[derive(Parser)]
#[command(
    name = "deltabench",
    about = "Physical storage analysis for Delta Lake snapshots",
    after_help = r#"
Examples:
  deltabench ./table
  deltabench ./table --version 42
  deltabench ./table --json
  deltabench ./table --d3 > treemap.html
"#
)]
struct Cli {
    /// local Delta table directory
    table: PathBuf,
    /// snapshot version (default: latest)
    #[arg(long)]
    version: Option<u64>,
    /// output format
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// shorthand for --format json
    #[arg(long, conflicts_with_all = ["d3", "format"])]
    json: bool,
    /// shorthand for --format d3
    #[arg(long, conflicts_with_all = ["json", "format"])]
    d3: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
    D3,
}

type CliError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Cli) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let report = runtime.block_on(deltabench::read_local(&args.table, args.version))?;
    let format = if args.json {
        Format::Json
    } else if args.d3 {
        Format::D3
    } else {
        args.format
    };
    let output = match format {
        Format::Text => deltabench::render(&report),
        Format::Json => deltabench::json(&report)?,
        Format::D3 => deltabench::render_html(&report)?,
    };
    print!("{output}");
    if !output.ends_with('\n') {
        println!();
    }
    Ok(())
}
