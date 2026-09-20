use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use pqbench::bytemass;
use pqbench::codecs::{Codec, CodecImpl};
use pqbench::compression;
use pqbench::parquet_helpers::PageParser;
use pqbench::stats;

#[cfg(feature = "delta")]
mod delta;

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
    /// analyze the active Parquet files in a local Delta snapshot
    #[cfg(feature = "delta")]
    #[command(after_help = "Example:\n  pqbench delta ./table --json")]
    Delta(delta::Args),
}

/// Arguments for `bytemass`.
#[derive(Args)]
struct BytemassArgs {
    /// parquet paths or glob masks; quote masks to prevent shell expansion
    #[arg(required = true)]
    inputs: Vec<String>,
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
        #[cfg(feature = "delta")]
        Command::Delta(args) => delta::run(&args),
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

/// Read Parquet files' column byte masses from their footers and output them as
/// text stats (agent-facing), JSON (composable), or, with `--d3`, as a
/// self-contained browser treemap. This is `bytemass` wired end-to-end.
fn run_bytemass(args: &BytemassArgs) -> Result<(), CliError> {
    let paths = expand_inputs(&args.inputs)?;
    let mass = summarize(&paths)?.file_mass();
    let raw = bytemass::read(&mass);
    let mut tree = bytemass::aggregate(&raw);
    tree.label = collection_label(&paths);
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

/// Measure every input; local paths go through the synchronous path, URIs
/// through the remote object reader (which needs an async runtime).
#[cfg(feature = "aws")]
fn summarize(paths: &[PathBuf]) -> Result<bytemass::MassSummary, CliError> {
    if !paths.iter().any(|path| has_uri_scheme(path)) {
        return Ok(bytemass::summarize_files(paths)?);
    }
    let inputs: Vec<String> = paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    Ok(runtime.block_on(bytemass::summarize_inputs(&inputs))?)
}

#[cfg(not(feature = "aws"))]
fn summarize(paths: &[PathBuf]) -> Result<bytemass::MassSummary, CliError> {
    if let Some(remote) = paths.iter().find(|path| has_uri_scheme(path)) {
        return Err(format!(
            "remote input {} requires the `aws` feature",
            remote.display()
        )
        .into());
    }
    Ok(bytemass::summarize_files(paths)?)
}

fn has_uri_scheme(path: &std::path::Path) -> bool {
    path.to_string_lossy().contains("://")
}

fn expand_inputs(inputs: &[String]) -> Result<Vec<PathBuf>, CliError> {
    let mut paths = BTreeSet::new();
    for input in inputs {
        if has_glob_metachar(input) {
            let mut matched = false;
            for entry in glob::glob(&escape_literal_brackets(input))? {
                paths.insert(entry?);
                matched = true;
            }
            if !matched {
                return Err(format!("mask matched no files: {input}").into());
            }
        } else {
            paths.insert(PathBuf::from(input));
        }
    }
    Ok(paths.into_iter().collect())
}

fn has_glob_metachar(input: &str) -> bool {
    input.contains(['*', '?'])
}

fn escape_literal_brackets(input: &str) -> String {
    input.replace('[', "[[]")
}

fn collection_label(paths: &[PathBuf]) -> String {
    if let [path] = paths {
        return path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".into());
    }
    format!("{} parquet files", paths.len())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_masks_and_rejects_empty_matches() {
        let mask = format!("{}/tests/fixtures/*.parquet", env!("CARGO_MANIFEST_DIR"));
        let paths = expand_inputs(&[mask]).unwrap();
        assert!(!paths.is_empty());

        let missing = format!("{}/tests/fixtures/*.missing", env!("CARGO_MANIFEST_DIR"));
        assert!(expand_inputs(&[missing]).is_err());
    }

    #[test]
    fn treats_brackets_as_literal_path_characters() {
        assert!(!has_glob_metachar("data/archive[1].parquet"));
        assert!(has_glob_metachar("data/archive?.parquet"));
        assert!(has_glob_metachar("data/*.parquet"));
        assert_eq!(
            escape_literal_brackets("data/part[1]/*.parquet"),
            "data/part[[]1]/*.parquet"
        );
    }

    #[test]
    fn labels_multiple_files_as_a_collection() {
        let paths = vec![PathBuf::from("a.parquet"), PathBuf::from("b.parquet")];
        assert_eq!(collection_label(&paths), "2 parquet files");
    }
}
