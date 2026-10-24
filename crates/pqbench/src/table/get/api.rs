//! `table::get`: one table's record.
//!
//! [`get`] is the only public function. It returns the [`Table`] resource —
//! the wire form of the loaded [`TableInfo`](crate::table::TableInfo) — and the
//! CLI forwards it as-is.

use std::collections::BTreeMap;
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
/// `uri` is where the ref already says the metadata is, when it does: the
/// storage read then starts alongside the catalog call, and the catalog's own
/// location stays authoritative. `env` is the `AWS_*` options that read needs;
/// empty reads under whatever the process environment holds. The caller, which
/// holds the lease, sets the returned record's `credentials`.
///
/// # Errors
/// Fails when `name` is not a table name, the endpoint cannot be reached,
/// answers with an unexpected status, or the table's metadata cannot be read.
pub async fn get(
    workspace: &Workspace,
    lake: &Lake,
    name: &str,
    uri: Option<&str>,
    env: &BTreeMap<String, String>,
) -> Result<Table, Error> {
    super::r#impl::get(workspace, lake, name, uri, env).await
}
