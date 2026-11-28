//! Presentation: serialize an lz [`report::Report`] as composition-friendly
//! JSON.

use crate::bench::Error;
use crate::report;

/// Serialize an lz report as pretty-printed JSON.
///
/// # Errors
/// Returns [`Error`] if serialization fails.
pub fn render_json(report: &report::Report) -> Result<String, Error> {
    Ok(serde_json::to_string_pretty(report)?)
}
