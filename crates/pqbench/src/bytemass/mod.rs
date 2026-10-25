//! `bytemass`: the per-column byte masses of a parquet file.
//!
//! The command is one function: [`bytemass`] takes a [`BytemassRequest`] and
//! returns the measured columns, a `Vec<v1::Bytemass>` with one record per
//! column. The CLI streams those records; `pqbench viz` collects them.
//!
//! Each file's footer is read once and its column chunks are folded over the
//! file's row groups into one [`v1::Bytemass`] per column.

mod api;
mod collection;
mod remote;

pub use api::{bytemass, measure_file, measure_files, BytemassRequest, MeasuredFile};

use std::fmt;

/// Errors measuring a file's byte masses.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// No inputs were given.
    NoInputs,
    /// A measured object was not a valid Parquet file.
    Invalid(String),
    /// The object store failed.
    Storage(crate::third_party::object_store::Error),
    /// The parquet layer failed.
    Parquet(crate::third_party::parquet::api::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NoInputs => write!(f, "no inputs"),
            Error::Invalid(message) => write!(f, "{message}"),
            Error::Storage(_) => write!(f, "object storage failed"),
            Error::Parquet(_) => write!(f, "the parquet layer failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Storage(error) => Some(error),
            Error::Parquet(error) => Some(error),
            Error::NoInputs | Error::Invalid(_) => None,
        }
    }
}

impl From<crate::third_party::parquet::api::Error> for Error {
    fn from(error: crate::third_party::parquet::api::Error) -> Self {
        Error::Parquet(error)
    }
}
