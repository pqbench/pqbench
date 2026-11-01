//! `bytemass`: the per-column byte masses of a Parquet file.
//!
//! [`list_columns`] lists one blob's columns, each carrying its
//! [`v1::Bytemass`](crate::v1::Bytemass) when the footer was measured. The
//! columns carry no table, file size, storage class, or sample rate: the caller
//! that measured them through a table file adds those. The CLI streams the
//! records; `pqbench summarize columns` folds them and `pqbench format --html`
//! draws the rows.
//!
//! Each file's footer is read once and its column chunks are folded over the
//! file's row groups into one mass per column.

mod api;
mod collection;
mod remote;

pub use api::list_columns;

use std::fmt;

/// Errors measuring a file's byte masses.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name was not a blob name.
    Name(String),
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
            Error::Name(message) => write!(f, "{message}"),
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
            Error::Name(_) | Error::Invalid(_) => None,
        }
    }
}

impl From<crate::third_party::parquet::api::Error> for Error {
    fn from(error: crate::third_party::parquet::api::Error) -> Self {
        Error::Parquet(error)
    }
}
