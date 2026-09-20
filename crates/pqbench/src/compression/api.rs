//! The `compression` command: one typed request in, one report out.
//!
//! The CLI parses its arguments into a [`CompressionRequest`], calls
//! [`compression`], and renders the report with [`render_text`] or
//! [`render_json`].

use std::path::PathBuf;

use crate::bench::{self, Error};
use crate::parquet_helpers::{default_parser, PageParser};
use crate::report;
use crate::stats;

/// Arguments for the `compression` command.
#[derive(Debug, Clone)]
pub struct CompressionRequest {
    /// Input file (NONE-compressed parquet).
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
    /// Report one row per column chunk in addition to the file-level rows.
    pub per_column: bool,
}

/// Sweep every codec×level in the request over a NONE-compressed parquet file's
/// encoded page payloads.
///
/// The file is read and parsed into per-column page buffers once; each codec×
/// level then compresses and decompresses every page payload, verifying each
/// round-trip. The result is one [`report::ReportRow`] per config, ordered by
/// compress speed, plus per-column rows when `per_column` is set.
///
/// # Errors
/// Fails when a `codec@level` spec is malformed, the file cannot be read or
/// parsed (only NONE-compressed input is supported), or a codec errors or fails
/// its round-trip.
pub fn compression(request: &CompressionRequest) -> Result<report::Report, Error> {
    let sweep = bench::resolve(
        &request.codec_specs,
        request.samples,
        request.warmup_iterations,
        request.mode,
    )?;
    let bytes = std::fs::read(&request.file).map_err(crate::codecs::Error::from)?;
    let parsed = default_parser().parse_pages(&bytes)?;
    let raw = super::raw::bench_file(&parsed, &sweep.codec_configs, sweep.passes)?;
    Ok(super::analytics::aggregate(
        &raw,
        &sweep.stats_config,
        request.per_column,
    ))
}
