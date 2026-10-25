//! `schema::list`: the schemas in one catalog.
//!
//! [`list`] is the only public function. It returns [`Schema`] resources; the
//! CLI forwards them as-is.

use std::fmt;

use futures_util::stream::{self, Stream, TryStreamExt};

pub use crate::config::Dialect as TableFormat;
pub use crate::v1::{Catalog, Schema};

use crate::Workspace;

/// Errors listing a catalog's schemas.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name argument was not a resource name.
    Name(String),
    /// The endpoint request failed.
    Http(crate::http::Error),
    /// The endpoint returned a structurally invalid document.
    Malformed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(message) | Error::Malformed(message) => write!(f, "{message}"),
            Error::Http(_) => write!(f, "the endpoint request failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            Error::Name(_) | Error::Malformed(_) => None,
        }
    }
}

/// List the schemas in the catalog `catalog` names, at the workspace's
/// endpoint.
///
/// The stream owns the catalog name, so it borrows the workspace and nothing
/// else; a failure listing a page is an `Err` item, so the records read before
/// it stand.
pub fn list<'a>(
    workspace: &'a Workspace,
    catalog: &Catalog,
) -> impl Stream<Item = Result<Schema, Error>> + 'a {
    let name = catalog.name.clone();
    stream::once(async move {
        super::r#impl::list(workspace, &name)
            .await
            .map(|rows| stream::iter(rows.into_iter().map(Ok)))
    })
    .try_flatten()
}
