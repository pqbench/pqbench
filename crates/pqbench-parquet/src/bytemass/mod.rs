//! `bytemass`: the per-column byte masses of a Parquet file.
//!
//! [`measure_footer`] reads one local file's footer and folds its column
//! chunks into one [`v1::Bytemass`](crate::v1::Bytemass) per leaf column;
//! [`from_file_mass`] is the same fold for a caller that already read the
//! footer somewhere else (a remote byte range, a cached copy). The CLI streams
//! the records; `pqbench summarize columns` folds them and
//! `pqbench format --html` draws the rows.
//!
//! Each file's footer is read once and its column chunks are folded over the
//! file's row groups into one mass per column.

mod collection;

pub use collection::from_file_mass;
pub(crate) use collection::measure_footer;

use std::fmt;

use crate::third_party::parquet::api;

/// One measured blob: its storage facts and one record per leaf column.
#[derive(Debug, Clone)]
pub struct MeasuredFile {
    /// The blob's name, size, storage class, and file-level counts.
    pub blob: crate::v1::Blob,
    /// One record per leaf column, in file order.
    pub columns: Vec<crate::v1::Column>,
}

/// Errors measuring a file's byte masses.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name was not a blob name.
    Name(String),
    /// A measured object was not a valid Parquet file.
    Invalid(String),
    /// The object store failed. The payload is whatever this build's storage
    /// layer returned; a caller that needs the store's own error type downcasts
    /// it, and the local paths never construct this variant.
    Storage(Box<dyn std::error::Error + Send + Sync>),
    /// The parquet layer failed.
    Parquet(api::Error),
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
            Error::Storage(error) => Some(&**error),
            Error::Parquet(error) => Some(error),
            Error::Name(_) | Error::Invalid(_) => None,
        }
    }
}

impl From<api::Error> for Error {
    fn from(error: api::Error) -> Self {
        Error::Parquet(error)
    }
}
