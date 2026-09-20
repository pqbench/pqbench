use std::path::PathBuf;

use clap::{Args, ValueEnum};
use pqbench::codecs::{Codec, CodecImpl};
use pqbench::stats;

use crate::CliError;

/// Arguments shared by every bench subcommand.
#[derive(Args)]
pub(crate) struct BenchArgs {
    /// input file
    pub(crate) file: PathBuf,
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
    pub(crate) per_column: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum ModeArg {
    Fastest,
    Mean,
}

/// What a bench run needs: the codec×level set, the analytics decisions, and
/// the raw pass count the upstream wants (warmup + samples).
pub(crate) struct BenchPlan {
    pub(crate) configs: Vec<(Codec, u8)>,
    pub(crate) cfg: stats::Config,
    pub(crate) passes: u32,
}

/// The measurement decisions shared by both commands: the codec×level set, the
/// analytics config (warmup + mode), and the raw pass count the upstream wants.
pub(crate) fn bench_plan(args: &BenchArgs) -> Result<BenchPlan, CliError> {
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
    let codec = parse_codec(name)?;
    Ok((codec, level.unwrap_or_else(|| default_level(codec))))
}

fn parse_codec(name: &str) -> Result<Codec, String> {
    Codec::from_name(name).ok_or_else(|| format!("unknown codec: {name}"))
}
