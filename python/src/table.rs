//! `table`: read one table, or list a schema's refs.

use crate::handles::{Lake, Workspace};
use crate::prelude::*;
use crate::records::{many, one, Table};

#[pyfunction]
#[pyo3(name = "get")]
fn table_get(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    lake: PyRef<'_, Lake>,
    name: String,
    uri: Option<String>,
    credentials: Option<Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    let lake = lake.0.clone();
    let credentials = crate::requests::credentials(credentials)?;
    one(
        py,
        async move {
            pqbench::table::get::get(
                &workspace,
                &lake,
                &name,
                uri.as_deref(),
                credentials.as_ref(),
            )
            .await
            .map_err(|error| error.to_string())
        },
        Table::attach,
    )
}
#[pyfunction]
#[pyo3(name = "list")]
fn table_list(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    name: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    many(
        py,
        async move {
            pqbench::table::list::list(&workspace, &name)
                .await
                .map_err(|error| error.to_string())
        },
        Table::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "table")?;
    let get = crate::submodule(py, &module, "get")?;
    get.add_function(wrap_pyfunction!(table_get, &get)?)?;
    let list = crate::submodule(py, &module, "list")?;
    list.add_function(wrap_pyfunction!(table_list, &list)?)?;
    crate::expose(py, "pqbench.native.table", &module)?;
    crate::expose(py, "pqbench.native.table.get", &get)?;
    crate::expose(py, "pqbench.native.table.list", &list)?;
    Ok(())
}
