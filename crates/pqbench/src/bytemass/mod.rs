//! `bytemass`: the per-column byte masses of a parquet file.
//!
//! The command is one function: [`bytemass`] takes a [`BytemassRequest`] and
//! returns the measured table, a `Vec<MassRow>` with one row per (file, column
//! chunk). Rendering is a fold of that table, one module per output:
//! [`render_text`] prints the stats as CLI text, [`render_json`] serializes the
//! per-column totals as flat, composable JSON, [`render_html`] folds the table
//! into a browser treemap, and [`aggregate`] sums it per column.
//!
//! The layers behind those are private: `raw` (`read`) carries the per-chunk
//! on-disk byte masses; `analytics` (`aggregate`) sums chunks across row groups
//! into a tree keyed on on-disk bytes per row, nested by column path. An
//! optional [`FileMassCache`] stores those masses by object identity so an
//! unchanged Parquet file is not footer-read again.

mod aggregate;
mod analytics;
mod api;
mod cache;
mod collection;
mod d3;
mod json;
mod raw;
mod remote;
mod text;

pub use aggregate::aggregate;
pub use api::{bytemass, bytemass_with_cache, BytemassRequest, MassRow};
pub use cache::FileMassCache;
pub use collection::{ColumnMassSummary, MassSummary};
pub use d3::render_html;
pub use json::render_json;
pub use text::render_text;
