//! `bytemass`: the per-column byte masses of a parquet file.
//!
//! Split by layer like `compression`: `raw` (`read`) carries the per-chunk
//! on-disk byte masses; `analytics` (`aggregate`) sums chunks across row groups
//! into a tree keyed on on-disk bytes per row, nested by column path; `json`
//! (`tree`) serializes that tree as composable `{name, value, children}` JSON;
//! `text` (`render`) prints the stats as CLI text for agentic calls; `d3`
//! (`render_html`) wraps the JSON in a self-contained browser treemap.

mod analytics;
mod d3;
mod json;
mod raw;
mod text;

pub use analytics::{aggregate, MassNode};
pub use d3::render_html;
pub use json::tree;
pub use raw::{read, FileRaw, RawColumn};
pub use text::render;
