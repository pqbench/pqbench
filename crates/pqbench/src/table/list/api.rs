//! `table::list`: the tables in one schema.
//!
//! [`list`] is the only public function. It returns [`Table`] resources; the
//! CLI forwards them as-is.

use std::fmt;

use futures_util::stream::{self, Stream, TryStreamExt};

pub use crate::v1::{Schema, Table};

use crate::Workspace;

/// Errors listing a schema's tables.
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

/// List the tables in the schema `schema` names, at the workspace's endpoint.
///
/// Unity pages carry a storage location; Iceberg REST lists identifiers only,
/// so `uri` is unset there until `pqbench get table` resolves it. The stream
/// owns the schema name, so it borrows the workspace and nothing else; a
/// failure listing a page is an `Err` item, so the records read before it stand.
pub fn list<'a>(
    workspace: &'a Workspace,
    schema: &Schema,
) -> impl Stream<Item = Result<Table, Error>> + 'a {
    let name = schema.name.clone();
    stream::once(async move {
        super::r#impl::list(workspace, &name)
            .await
            .map(|rows| stream::iter(rows.into_iter().map(Ok)))
    })
    .try_flatten()
}
