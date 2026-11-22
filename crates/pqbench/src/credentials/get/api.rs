//! Vending one table's read credentials for the lease daemon.

use std::fmt;

use crate::v1::{Credentials, Table};
use crate::{Dialect, Workspace};

/// Errors vending a table's credentials.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The name argument was not a resource name.
    Name(String),
    /// The endpoint request failed.
    Http(crate::http::Error),
    /// The catalog marks the table's storage unreadable outside its own
    /// compute, so no lease can be minted for it and the process's own
    /// credentials will not do either.
    Refused { name: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(message) => write!(f, "{message}"),
            Error::Http(_) => write!(f, "the endpoint request failed"),
            Error::Refused { name } => write!(
                f,
                "the catalog marks {name} TABLE_DB_STORAGE: its files are unreadable outside the \
                 workspace's compute"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            Error::Name(_) | Error::Refused { .. } => None,
        }
    }
}

/// Vend the read credentials of the table `name` — a
/// `catalogs/{catalog}/schemas/{schema}/tables/{table}` resource name; `None`
/// when the catalog vends nothing for it.
///
/// The dialect decides the route: Unity's `temporary-table-credentials`, or the
/// Iceberg REST catalog's `storage-credentials`. A table the catalog vends
/// nothing for — a catalog that serves no vending route, a serverless notebook
/// whose compute reaches storage itself — has none, so a caller keeps its own.
///
/// # Errors
/// Fails for a name that is not a table, when the endpoint cannot be reached,
/// when it answers an unexpected status, and for a table whose storage the
/// catalog marks unreadable outside its own compute: no lease can be minted
/// for it, and a caller must not fall back to its own credentials.
pub async fn get(workspace: &Workspace, name: &str) -> Result<Option<Credentials>, Error> {
    let (catalog, schema, table_name) =
        Table::parse_name(name).ok_or_else(|| Error::Name(format!("not a table name: {name}")))?;
    match workspace.config().dialect {
        Dialect::Unity => {
            super::r#impl::vend_unity(workspace, &catalog, &schema, &table_name).await
        }
        Dialect::Iceberg => {
            super::r#impl::vend_iceberg(workspace, &catalog, &schema, &table_name).await
        }
    }
}
