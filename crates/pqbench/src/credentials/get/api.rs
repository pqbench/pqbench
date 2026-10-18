//! `credentials get`: a table-ref's vended read credentials, on the ref itself.

use std::fmt;

use crate::v1::{name, Credentials, Table};
use crate::{Dialect, Workspace};

/// Errors vending a table's credentials.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "credentials: {}", self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

/// Vend `table`'s read credentials and write them onto the ref as its
/// [`Table::credentials`] — the walk's `list tables | credentials get`.
///
/// The dialect decides the route: Unity's `temporary-table-credentials`, or the
/// Iceberg REST catalog's `storage-credentials`. A table the catalog vends
/// nothing for — managed default storage, a view, a serverless notebook whose
/// compute reaches storage itself — comes back unchanged, so the caller keeps
/// its own `env`.
///
/// # Errors
/// Fails for a name that is not a table, when the endpoint cannot be reached,
/// or when it answers an unexpected status.
pub async fn get(workspace: &Workspace, table: &Table) -> Result<Table, Error> {
    let (catalog, schema, table_name) = name::parse_table(&table.name)
        .ok_or_else(|| Error(format!("not a table name: {}", table.name)))?;
    let env = match workspace.config().dialect {
        Dialect::Unity => {
            super::r#impl::vend_unity(workspace, &catalog, &schema, &table_name).await?
        }
        Dialect::Iceberg => super::r#impl::vend_iceberg(workspace, &schema, &table_name).await?,
    };
    let Some(env) = env else {
        return Ok(table.clone());
    };
    let credentials = Credentials::from_env(&env);
    if credentials.is_empty() {
        return Ok(table.clone());
    }
    Ok(Table {
        credentials: Some(credentials),
        ..table.clone()
    })
}
