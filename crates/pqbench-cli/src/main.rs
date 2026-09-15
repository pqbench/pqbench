use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use pqbench::bytemass;
use pqbench::codecs::{Codec, CodecImpl};
use pqbench::compression;
use pqbench::parquet_helpers::{MetadataParser, PageParser};
use pqbench::stats;

#[derive(Parser)]
#[command(
    name = "pqbench",
    about = "lzbench for parquet",
    after_help = r#"
Examples:
  pqbench lz file.bin -c zstd@3 --samples 10
  pqbench compression data.parquet --per-column
  pqbench bytemass data.parquet
  pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
"#
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Arguments shared by every bench subcommand.
#[derive(Args)]
struct BenchArgs {
    /// input file
    file: PathBuf,
    /// codec@level, repeatable; default: all wired codecs
    #[arg(short, value_name = "codec@level")]
    codec: Vec<String>,
    /// timed passes to collect per sweep (after warmup)
    #[arg(long, default_value_t = 10)]
    samples: u32,
    /// timed passes to discard before sampling (cold-start effects)
    #[arg(long, default_value_t = 3)]
    warmup_iterations: u32,
    /// how to reduce the samples: fastest = mean over the best pass, mean = mean over all
    #[arg(long, value_enum, default_value_t = ModeArg::Fastest)]
    mode: ModeArg,
    /// report per-column breakdown (compression only)
    #[arg(long)]
    per_column: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum ModeArg {
    Fastest,
    Mean,
}

#[derive(Subcommand)]
enum Command {
    /// lzbench-style compression benchmark over raw file bytes
    Lz(BenchArgs),
    /// lzbench-style codec sweep over encoded parquet pages (NONE-compressed input)
    Compression(BenchArgs),
    /// export per-column byte masses (on-disk bytes/row)
    #[command(after_help = r#"
Examples:
  pqbench bytemass data.parquet
  pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
"#)]
    Bytemass(BytemassArgs),
}

/// Arguments for `bytemass`.
#[derive(Args)]
struct BytemassArgs {
    /// input parquet file
    file: PathBuf,
    /// emit the byte-mass tree as JSON (composable) instead of text stats
    #[arg(long, conflicts_with = "d3")]
    json: bool,
    /// emit a self-contained d3 treemap HTML (open in a browser) instead of text stats
    #[arg(long)]
    d3: bool,
}

/// The CLI's single error channel: any error from the io, parquet, or codec
/// layers, converted via `?`.
type CliError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Lz(args) => run_lz(&args),
        Command::Compression(args) => run_compression(&args),
        Command::Bytemass(args) => run_bytemass(&args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run_lz(args: &BenchArgs) -> Result<(), CliError> {
    let plan = bench_plan(args)?;
    let raw = pqbench::lz::bench_file(&args.file, &plan.configs, plan.passes)?;
    pqbench::lz::render(&pqbench::lz::aggregate(&raw, &plan.cfg));
    Ok(())
}

/// Read a NONE-compressed parquet file, parse its pages, and sweep every config
/// over them. This is `compression` wired end-to-end.
fn run_compression(args: &BenchArgs) -> Result<(), CliError> {
    let plan = bench_plan(args)?;
    let bytes = std::fs::read(&args.file)?;
    let parsed = pqbench::parquet_helpers::default_parser().parse_pages(&bytes)?;
    let raw = compression::bench_file(&parsed, &plan.configs, plan.passes)?;
    compression::render(
        &compression::aggregate(&raw, &plan.cfg, args.per_column),
        args.per_column,
    );
    Ok(())
}

/// Read a parquet file's column byte masses from its footer and output them as
/// text stats (agent-facing), JSON (composable), or, with `--d3`, as a
/// self-contained browser treemap. This is `bytemass` wired end-to-end.
fn run_bytemass(args: &BytemassArgs) -> Result<(), CliError> {
    let mass = pqbench::parquet_helpers::default_metadata_parser().read_masses(&args.file)?;
    let raw = bytemass::read(&mass);
    let mut tree = bytemass::aggregate(&raw);
    tree.label = args
        .file
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".into());
    let out = if args.json {
        bytemass::tree(&tree)?
    } else if args.d3 {
        bytemass::render_html(&tree)?
    } else {
        bytemass::render(&tree)
    };
    print!("{out}");
    Ok(())
}

/// What a bench run needs: the codec×level set, the analytics decisions, and
/// the raw pass count the upstream wants (warmup + samples).
struct BenchPlan {
    configs: Vec<(Codec, u8)>,
    cfg: stats::Config,
    passes: u32,
}

/// The measurement decisions shared by both commands: the codec×level set, the
/// analytics config (warmup + mode), and the raw pass count the upstream wants.
fn bench_plan(args: &BenchArgs) -> Result<BenchPlan, CliError> {
    let cfg = stats::Config {
        warmup_iterations: args.warmup_iterations as usize,
        mode: match args.mode {
            ModeArg::Fastest => stats::Mode::Fastest,
            ModeArg::Mean => stats::Mode::Mean,
        },
    };
    Ok(BenchPlan {
        configs: parse_configs(&args.codec)?,
        cfg,
        passes: args.samples + args.warmup_iterations,
    })
}

fn default_level(codec: Codec) -> u8 {
    codec.level_range().first_level as u8
}

fn parse_configs(specs: &[String]) -> Result<Vec<(Codec, u8)>, String> {
    if specs.is_empty() {
        return Ok(Codec::all().map(|c| (c, default_level(c))).collect());
    }
    specs.iter().map(|spec| parse_spec(spec)).collect()
}

/// One `codec@level` spec. The `@level` part is optional and defaults to the
/// codec's lowest level.
fn parse_spec(spec: &str) -> Result<(Codec, u8), String> {
    let (name, level) = match spec.split_once('@') {
        Some((n, l)) => (
            n,
            Some(
                l.parse::<u8>()
                    .map_err(|_| format!("bad level in {spec}"))?,
            ),
        ),
        None => (spec, None),
    };
    let codec = Codec::from_name(name).ok_or_else(|| format!("unknown codec: {name}"))?;
    Ok((codec, level.unwrap_or_else(|| default_level(codec))))
}
