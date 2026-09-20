//! Presentation: render an lz [`report::Report`] to a text table.

use crate::bench::Error;
use crate::report;
use crate::text;

/// Render an lz report's file-level rows.
pub fn render_text(report: &report::Report) -> Result<String, Error> {
    let mut out = String::new();
    text::render_rows(&mut out, report);
    Ok(out)
}
