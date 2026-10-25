//! `catalog::list`: the catalogs at the endpoint.
//!
//! [`list`] is the only public function. It returns [`Catalog`] resources; the
//! CLI forwards them as-is.

use std::fmt;

use futures_util::stream::{self, Stream, TryStreamExt};

pub use crate::v1::Catalog;

use crate::Workspace;

/// Errors listing catalogs.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name argument was not a resource name.
    Name(String),
    /// The endpoint request failed.
    Http(crate::http::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(message) => write!(f, "{message}"),
            Error::Http(_) => write!(f, "the endpoint request failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            Error::Name(_) => None,
        }
    }
}

/// List the catalogs at the workspace's endpoint, in name order.
///
/// The stream borrows the workspace and nothing else; a failure listing a page
/// is an `Err` item, so the records read before it stand.
pub fn list(workspace: &Workspace) -> impl Stream<Item = Result<Catalog, Error>> + '_ {
    stream::once(async move {
        super::r#impl::list(workspace)
            .await
            .map(|rows| stream::iter(rows.into_iter().map(Ok)))
    })
    .try_flatten()
}
