//! The request and codec-spec parsing shared by the `lz` and `compression`
//! sweeps.
//!
//! Both commands share the same arguments (`file`, `-c codec@level`, samples,
//! warmup, mode); a [`BenchRequest`] is the typed form of those arguments. The
//! command modules ([`crate::lz`], [`crate::compression`]) resolve the request
//! into a [`BenchPlan`] and run their own measurement, so parsing lives here
//! once and the CLI stays a thin wrapper.

use std::path::PathBuf;

use crate::codecs::{Codec, CodecImpl, Error as CodecError};
use crate::stats;

/// Arguments shared by the `lz` and `compression` commands.
#[derive(Debug, Clone)]
pub struct BenchRequest {
    /// Input file.
    pub file: PathBuf,
    /// `codec@level` specs, repeatable; empty selects every wired codec at its
    /// first level. The `@level` part is optional.
    pub codec_specs: Vec<String>,
    /// Timed passes to collect per sweep (after warmup).
    pub samples: u32,
    /// Timed passes to discard before sampling (cold-start effects).
    pub warmup_iterations: u32,
    /// How to reduce the samples.
    pub mode: stats::Mode,
}

/// The measurement decisions a [`BenchRequest`] resolves to: the codec×level
/// set, the analytics config, and the raw pass count the upstream wants.
pub(crate) struct BenchPlan {
    pub(crate) codec_configs: Vec<(Codec, u8)>,
    pub(crate) stats_config: stats::Config,
    pub(crate) passes: u32,
}

impl BenchRequest {
    /// Resolve the request into codec configs, an analytics config, and the raw
    /// pass count (warmup + samples).
    pub(crate) fn plan(&self) -> Result<BenchPlan, Error> {
        Ok(BenchPlan {
            codec_configs: parse_codec_configs(&self.codec_specs)?,
            stats_config: stats::Config {
                warmup_iterations: self.warmup_iterations as usize,
                mode: self.mode,
            },
            passes: self.samples + self.warmup_iterations,
        })
    }
}

/// Errors from a codec sweep: spec parsing, the codec/parquet layers, or
/// rendering.
#[derive(Debug)]
pub enum Error {
    /// A `codec@level` spec could not be parsed.
    Spec(String),
    /// The codec layer failed.
    Codec(CodecError),
    /// The parquet layer failed.
    Parquet(crate::parquet_helpers::Error),
    /// A renderer could not serialize its output.
    Json(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Spec(e) => write!(f, "{e}"),
            Error::Codec(e) => write!(f, "{e}"),
            Error::Parquet(e) => write!(f, "{e}"),
            Error::Json(e) => write!(f, "json: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<CodecError> for Error {
    fn from(e: CodecError) -> Self {
        Error::Codec(e)
    }
}

impl From<crate::parquet_helpers::Error> for Error {
    fn from(e: crate::parquet_helpers::Error) -> Self {
        Error::Parquet(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

/// Parse the `codec@level` specs; empty selects every wired codec at its first
/// level.
fn parse_codec_configs(specs: &[String]) -> Result<Vec<(Codec, u8)>, Error> {
    if specs.is_empty() {
        return Ok(Codec::all().map(|c| (c, default_level(c))).collect());
    }
    specs.iter().map(|spec| parse_spec(spec)).collect()
}

/// One `codec@level` spec. The `@level` part is optional and defaults to the
/// codec's lowest level.
fn parse_spec(spec: &str) -> Result<(Codec, u8), Error> {
    let Some((name, level)) = spec.split_once('@') else {
        let codec = parse_codec(spec)?;
        return Ok((codec, default_level(codec)));
    };
    let level = level
        .parse::<u8>()
        .map_err(|_| Error::Spec(format!("bad level in {spec}")))?;
    Ok((parse_codec(name)?, level))
}

fn parse_codec(name: &str) -> Result<Codec, Error> {
    Codec::from_name(name).ok_or_else(|| Error::Spec(format!("unknown codec: {name}")))
}

fn default_level(codec: Codec) -> u8 {
    codec.level_range().first_level as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_specs_select_every_codec_at_its_first_level() {
        let configs = parse_codec_configs(&[]).unwrap();
        assert_eq!(configs.len(), Codec::all().count());
        for (codec, level) in configs {
            assert_eq!(level, default_level(codec));
        }
    }

    #[test]
    fn parses_an_explicit_level() {
        assert_eq!(parse_spec("zstd@3").unwrap(), (Codec::Zstd, 3));
    }

    #[test]
    fn missing_level_defaults_to_the_first_level() {
        assert_eq!(
            parse_spec("snappy").unwrap(),
            (Codec::Snappy, default_level(Codec::Snappy))
        );
    }

    #[test]
    fn unknown_codec_and_bad_level_are_spec_errors() {
        assert!(matches!(parse_spec("nope"), Err(Error::Spec(_))));
        assert!(matches!(parse_spec("zstd@x"), Err(Error::Spec(_))));
    }

    #[test]
    fn plan_sums_warmup_and_samples_into_passes() {
        let request = BenchRequest {
            file: PathBuf::from("x"),
            codec_specs: vec!["zstd@1".into()],
            samples: 10,
            warmup_iterations: 3,
            mode: stats::Mode::Mean,
        };
        let plan = request.plan().unwrap();
        assert_eq!(plan.passes, 13);
        assert_eq!(plan.stats_config.warmup_iterations, 3);
        assert_eq!(plan.stats_config.mode, stats::Mode::Mean);
        assert_eq!(plan.codec_configs, [(Codec::Zstd, 1)]);
    }
}
