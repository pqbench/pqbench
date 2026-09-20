use std::path::PathBuf;

use clap::{Args, ValueEnum};
use pqbench::stats;

/// Arguments shared by the `lz` and `compression` sweeps.
#[derive(Args)]
pub(crate) struct BenchArgs {
    /// input file
    pub(crate) file: PathBuf,
    /// codec@level, repeatable; default: all wired codecs
    #[arg(short, value_name = "codec@level")]
    pub(crate) codec_specs: Vec<String>,
    /// timed passes to collect per sweep (after warmup)
    #[arg(long, default_value_t = 10)]
    pub(crate) samples: u32,
    /// timed passes to discard before sampling (cold-start effects)
    #[arg(long, default_value_t = 3)]
    pub(crate) warmup_iterations: u32,
    /// how to reduce the samples: fastest = mean over the best pass, mean = mean over all
    #[arg(long, value_enum, default_value_t = BenchMode::Fastest)]
    pub(crate) mode: BenchMode,
    /// emit the report as JSON (composable) instead of a text table
    #[arg(long = "json")]
    pub(crate) json: bool,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum BenchMode {
    Fastest,
    Mean,
}

impl From<BenchMode> for stats::Mode {
    fn from(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Fastest => stats::Mode::Fastest,
            BenchMode::Mean => stats::Mode::Mean,
        }
    }
}
