//! The `lz` command: one typed request in, one report out.
//!
//! The CLI parses its arguments into an [`LzRequest`], awaits [`lz`], and
//! renders the report with [`render_text`] or [`render_json`].

use std::path::PathBuf;

use crate::bench::{self, Error};
use crate::report;
use crate::stats;

/// Arguments for the `lz` command.
#[derive(Debug, Clone)]
pub struct LzRequest {
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

/// Bench raw file bytes under every codec×level in the request.
///
/// The file is loaded once and the codec sweep runs over its raw bytes
/// (lzbench's model), verifying each round-trip. The result is one
/// [`report::ReportRow`] per config, ordered by compress speed.
///
/// # Errors
/// Fails when a `codec@level` spec is malformed, the file cannot be read, or a
/// codec errors or fails its round-trip.
pub fn lz(request: &LzRequest) -> Result<report::Report, Error> {
    let sweep = bench::resolve(
        &request.codec_specs,
        request.samples,
        request.warmup_iterations,
        request.mode,
    )?;
    let raw = super::raw::bench_file(&request.file, &sweep.codec_configs, sweep.passes)?;
    Ok(super::analytics::aggregate(&raw, &sweep.stats_config))
}
