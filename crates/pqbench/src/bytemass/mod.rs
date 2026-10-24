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
mod pages;
pub use pages::{scan_pages, PageRecord};
mod raw;
mod remote;
mod text;

pub use aggregate::aggregate;
pub use api::{
    bytemass, expand_inputs, measure_files, BytemassRequest, FileStat, MassRow, MeasuredFile,
};
pub use collection::{ColumnMassSummary, MassSummary};
pub use json::render_json;
pub use text::render_text;

use std::fmt;

/// Errors measuring a file's byte masses.
#[derive(Debug)]
pub enum Error {
    /// No inputs were given.
    NoInputs,
    /// A glob mask matched no files.
    NoMatch(String),
    /// A glob mask did not parse.
    Pattern {
        /// The mask that failed to parse.
        mask: String,
        /// The underlying glob failure.
        source: glob::PatternError,
    },
    /// An input could not be read.
    Io {
        /// What the call was doing (`"cannot stat"`, `"cannot canonicalize"`).
        context: &'static str,
        /// The input involved.
        input: String,
        /// The underlying I/O failure.
        source: std::io::Error,
    },
    /// A measured object was not a valid Parquet file.
    Invalid(String),
    /// The object store failed.
    Storage(crate::third_party::object_store::Error),
    /// The parquet layer failed.
    Parquet(crate::third_party::parquet::api::Error),
    /// A byte or row total exceeded its integer type.
    Overflow,
    /// The measured rows could not be serialized.
    Json(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NoInputs => write!(f, "no inputs"),
            Error::NoMatch(mask) => write!(f, "mask matched no files: {mask}"),
            Error::Pattern { mask, .. } => write!(f, "invalid mask {mask}"),
            Error::Io { context, input, .. } => write!(f, "{context} {input}"),
            Error::Invalid(message) => write!(f, "{message}"),
            Error::Storage(_) => write!(f, "object storage failed"),
            Error::Parquet(_) => write!(f, "the parquet layer failed"),
            Error::Overflow => write!(f, "metadata totals exceed u64"),
            Error::Json(_) => write!(f, "could not serialize the rows"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Pattern { source, .. } => Some(source),
            Error::Io { source, .. } => Some(source),
            Error::Storage(error) => Some(error),
            Error::Parquet(error) => Some(error),
            Error::Json(error) => Some(error),
            Error::NoInputs | Error::NoMatch(_) | Error::Invalid(_) | Error::Overflow => None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Json(error)
    }
}

impl From<crate::third_party::parquet::api::Error> for Error {
    fn from(error: crate::third_party::parquet::api::Error) -> Self {
        Error::Parquet(error)
    }
}
