//! The `lz` command: one typed request in, one report out.
//!
//! The request is the shared [`BenchRequest`]; the CLI parses its arguments
//! into one, awaits [`lz`], and renders the report with [`render_text`] or
//! [`render_json`].

use crate::bench::{BenchRequest, Error};
use crate::report;

/// Bench raw file bytes under every codec×level in the request.
///
/// The file is loaded once and the codec sweep runs over its raw bytes
/// (lzbench's model), verifying each round-trip. The result is one
/// [`report::ReportRow`] per config, ordered by compress speed.
///
/// # Errors
/// Fails when a `codec@level` spec is malformed, the file cannot be read, or a
/// codec errors or fails its round-trip.
pub fn lz(request: &BenchRequest) -> Result<report::Report, Error> {
    let plan = request.plan()?;
    let raw = super::raw::bench_file(&request.file, &plan.codec_configs, plan.passes)?;
    Ok(super::analytics::aggregate(&raw, &plan.stats_config))
}
