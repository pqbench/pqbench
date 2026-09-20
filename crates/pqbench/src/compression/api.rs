//! The `compression` command: one typed request in, one report out.
//!
//! The request wraps the shared [`BenchRequest`] with the compression-only
//! `per_column` option. The CLI parses its arguments into one, awaits
//! [`compression`], and renders the report with [`render_text`] or
//! [`render_json`].

use crate::bench::{BenchRequest, Error};
use crate::parquet_helpers::{default_parser, PageParser};
use crate::report;

/// Arguments for the `compression` command.
#[derive(Debug, Clone)]
pub struct CompressionRequest {
    /// The shared sweep arguments.
    pub bench: BenchRequest,
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
    let plan = request.bench.plan()?;
    let bytes = std::fs::read(&request.bench.file).map_err(crate::codecs::Error::from)?;
    let parsed = default_parser().parse_pages(&bytes)?;
    let raw = super::raw::bench_file(&parsed, &plan.codec_configs, plan.passes)?;
    Ok(super::analytics::aggregate(
        &raw,
        &plan.stats_config,
        request.per_column,
    ))
}
