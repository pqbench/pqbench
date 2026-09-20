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
    codec_specs: Vec<String>,
    /// timed passes to collect per sweep (after warmup)
    #[arg(long, default_value_t = 10)]
    samples: u32,
    /// timed passes to discard before sampling (cold-start effects)
    #[arg(long, default_value_t = 3)]
    warmup_iterations: u32,
    /// how to reduce the samples: fastest = mean over the best pass, mean = mean over all
    #[arg(long, value_enum, default_value_t = BenchMode::Fastest)]
    mode: BenchMode,
    /// report per-column breakdown (compression only)
    #[arg(long = "per-column")]
    pub(crate) is_per_column: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum BenchMode {
    Fastest,
    Mean,
}

/// What a bench run needs: the codec×level set, the analytics decisions, and
/// the raw pass count the upstream wants (warmup + samples).
pub(crate) struct BenchPlan {
    pub(crate) codec_configs: Vec<(Codec, u8)>,
    pub(crate) stats_config: stats::Config,
    pub(crate) passes: u32,
}

/// The measurement decisions shared by both commands: the codec×level set, the
/// analytics config (warmup + mode), and the raw pass count the upstream wants.
pub(crate) fn bench_plan(args: &BenchArgs) -> Result<BenchPlan, CliError> {
    let stats_config = stats::Config {
        warmup_iterations: args.warmup_iterations as usize,
        mode: match args.mode {
            BenchMode::Fastest => stats::Mode::Fastest,
            BenchMode::Mean => stats::Mode::Mean,
        },
    };
    Ok(BenchPlan {
        codec_configs: parse_codec_configs(&args.codec_specs)?,
        stats_config,
        passes: args.samples + args.warmup_iterations,
    })
}

fn default_level(codec: Codec) -> u8 {
    codec.level_range().first_level as u8
}

fn parse_codec_configs(specs: &[String]) -> Result<Vec<(Codec, u8)>, String> {
    if specs.is_empty() {
        return Ok(Codec::all().map(|c| (c, default_level(c))).collect());
    }
    specs.iter().map(|spec| parse_spec(spec)).collect()
}

/// One `codec@level` spec. The `@level` part is optional and defaults to the
/// codec's lowest level.
fn parse_spec(spec: &str) -> Result<(Codec, u8), String> {
    let Some((name, level)) = spec.split_once('@') else {
        let codec = parse_codec(spec)?;
        return Ok((codec, default_level(codec)));
    };
    let level = level
        .parse::<u8>()
        .map_err(|_| format!("bad level in {spec}"))?;
    Ok((parse_codec(name)?, level))
}

fn parse_codec(name: &str) -> Result<Codec, String> {
    Codec::from_name(name).ok_or_else(|| format!("unknown codec: {name}"))
}
