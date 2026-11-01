//! `table::get`: one table's record.
//!
//! [`get`] is the only public function. It returns the [`Table`] resource —
//! the wire form of the loaded [`TableInfo`](crate::table::TableInfo) — and the
//! CLI forwards it as-is.

use std::fmt;

pub use crate::config::Dialect as TableFormat;
pub use crate::v1::Table;

use crate::{Lake, Workspace};

/// Errors reading a table's record.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name argument was not a resource name.
    Name(String),
    /// The endpoint request failed.
    Http(crate::http::Error),
    /// The endpoint returned a structurally invalid document.
    Malformed(String),
    /// The table's metadata at the catalog-named location could not be read.
    Metadata {
        /// The table's full name.
        name: String,
        /// The location the catalog named.
        location: String,
        /// The loader failure.
        source: crate::table::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(message) | Error::Malformed(message) => write!(f, "{message}"),
            Error::Http(_) => write!(f, "the endpoint request failed"),
            Error::Metadata { name, location, .. } => {
                write!(f, "cannot read {name}'s metadata at {location}")?;
                if location.starts_with("s3://") {
                    write!(
                        f,
                        " (s3:// needs credentials on the lake source; Databricks default \
                         storage cannot be read outside Databricks compute)"
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            Error::Metadata { source, .. } => Some(source),
            Error::Name(_) | Error::Malformed(_) => None,
        }
    }
}

/// Read the record of the table `name` — a
/// `catalogs/{catalog}/schemas/{schema}/tables/{table}` resource name — at the
/// workspace's endpoint.
///
/// The catalog names the table and the location to load; the table's own
/// metadata is read without files (`without_files()` for Delta, the metadata
/// JSON only for Iceberg), so the cost is O(1) in files.
///
/// `table` carries the name and the location the ref already says, so the
/// storage read starts alongside the catalog call and the catalog's own
/// location stays authoritative. The caller scopes `lake` with the table's
/// lease (`Lake::with_lease`); the credentials are not copied onto the answer.
///
/// # Errors
/// Fails when the name is not a table name, the endpoint cannot be reached,
/// answers with an unexpected status, or the table's metadata cannot be read.
pub async fn get(workspace: &Workspace, lake: &Lake, table: &Table) -> Result<Table, Error> {
    let uri = table.uri.as_deref().filter(|uri| !uri.is_empty());
    super::r#impl::get(workspace, lake, &table.name, uri).await
}
