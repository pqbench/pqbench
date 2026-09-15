//! Presentation: render an lz [`report::Report`] to the terminal.

use crate::report;
use crate::text;

/// Render an lz report's file-level rows.
pub fn render(report: &report::Report) {
    text::render_rows(report);
}
