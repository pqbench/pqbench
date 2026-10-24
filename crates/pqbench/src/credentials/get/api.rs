//! `credentials get`: one table's vended read credentials.

use std::fmt;

use crate::v1::{name, Credentials};
use crate::{Dialect, Workspace};

/// Errors vending a table's credentials.
#[derive(Debug)]
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
            Error::Http(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            _ => None,
        }
    }
}

/// Vend the read credentials of the table `name` — a
/// `catalogs/{catalog}/schemas/{schema}/tables/{table}` resource name; `None`
/// when the catalog vends nothing for it.
///
/// The dialect decides the route: Unity's `temporary-table-credentials`, or the
/// Iceberg REST catalog's `storage-credentials`. A table the catalog vends
/// nothing for — managed default storage, a view, a serverless notebook whose
/// compute reaches storage itself — has none, so a caller keeps its own.
///
/// # Errors
/// Fails for a name that is not a table, when the endpoint cannot be reached,
/// or when it answers an unexpected status.
pub async fn get(workspace: &Workspace, name: &str) -> Result<Option<Credentials>, Error> {
    let (catalog, schema, table_name) =
        name::parse_table(name).ok_or_else(|| Error::Name(format!("not a table name: {name}")))?;
    let env = match workspace.config().dialect {
        Dialect::Unity => {
            super::r#impl::vend_unity(workspace, &catalog, &schema, &table_name).await?
        }
        Dialect::Iceberg => super::r#impl::vend_iceberg(workspace, &schema, &table_name).await?,
    };
    let Some(env) = env else {
        return Ok(None);
    };
    let credentials = Credentials::from_env(&env);
    Ok((!credentials.is_empty()).then_some(credentials))
}
