use std::path::PathBuf;

use clap::{Args, ValueEnum};
use pqbench::bench::BenchRequest;
use pqbench::stats;

/// Arguments shared by the `lz` and `compression` sweeps.
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
    /// emit the report as JSON (composable) instead of a text table
    #[arg(long = "json")]
    pub(crate) is_json: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum BenchMode {
    Fastest,
    Mean,
}

impl BenchArgs {
    /// Build the typed library request from the parsed arguments.
    pub(crate) fn request(&self) -> BenchRequest {
        BenchRequest {
            file: self.file.clone(),
            codec_specs: self.codec_specs.clone(),
            samples: self.samples,
            warmup_iterations: self.warmup_iterations,
            mode: match self.mode {
                BenchMode::Fastest => stats::Mode::Fastest,
                BenchMode::Mean => stats::Mode::Mean,
            },
        }
    }
}
