//! `bytemass`: the per-column byte masses of a parquet file.
//!
//! The command is one function: [`bytemass`] takes a [`BytemassRequest`] and
//! returns a typed [`BytemassOutput`], whose `Display` selects the text stats,
//! the composable JSON tree, or the d3 treemap page.
//!
//! The layers behind it are private: `raw` (`read`) carries the per-chunk
//! on-disk byte masses; `analytics` (`aggregate`) sums chunks across row groups
//! into a tree keyed on on-disk bytes per row, nested by column path; `json`
//! (`tree`) serializes that tree as composable `{name, value, children}` JSON;
//! `text` (`render`) prints the stats as CLI text for agentic calls; `d3`
//! (`render_html`) wraps the JSON in a self-contained browser treemap.

mod analytics;
mod collection;
mod command;
mod d3;
mod json;
mod raw;
mod remote;
mod text;

pub use analytics::MassNode;
pub use collection::{ColumnMassSummary, MassSummary};
pub use command::{bytemass, BytemassOutput, BytemassRequest, FileMassRecord};
