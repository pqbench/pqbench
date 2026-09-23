//! `bytemass`: the per-column byte masses of a parquet file.
//!
//! The command is one function: [`bytemass`] takes a [`BytemassRequest`] and
//! returns the measured table, a `Vec<MassRow>` with one row per (file, column
//! chunk). The CLI streams those rows. [`render_text`] and [`render_json`]
//! fold the table; [`aggregate`] sums it per column. `pqbench viz` collects
//! the stream into a static HTML treemap.
//!
//! The layers behind those are private: `raw` (`read`) carries the per-chunk
//! on-disk byte masses; `analytics` (`aggregate`) sums chunks across row groups
//! into a tree keyed on on-disk bytes per row, nested by column path.

mod aggregate;
mod analytics;
mod api;
mod collection;
mod json;
mod raw;
mod remote;
mod text;

pub use aggregate::aggregate;
pub use api::{bytemass, BytemassRequest, MassRow};
pub use collection::{ColumnMassSummary, MassSummary};
pub use json::render_json;
pub use text::render_text;
