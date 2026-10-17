//! `table`: read one table, or list a schema's refs.

use crate::handles::{Lake, Workspace};
use crate::marshal::records;
use crate::prelude::*;
use crate::requests::Storage;

#[pyfunction]
#[pyo3(name = "read")]
fn table_read(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    lake: PyRef<'_, Lake>,
    catalog: String,
    schema: String,
    table: String,
    storage: &Storage,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    let lake = lake.0.clone();
    let storage = LibStorage::from(storage);
    records(py, async move {
        pqbench::table::get::read(&workspace, &lake, &catalog, &schema, &table, &storage)
            .await
            .map_err(|error| error.to_string())
    })
}
#[pyfunction]
#[pyo3(name = "list")]
fn table_list(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
    schema: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::table::ls::list(&workspace, &catalog, &schema)
            .await
            .map_err(|error| error.to_string())
    })
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "table")?;
    let get = crate::submodule(py, &module, "get")?;
    get.add_function(wrap_pyfunction!(table_read, &get)?)?;
    let ls = crate::submodule(py, &module, "ls")?;
    ls.add_function(wrap_pyfunction!(table_list, &ls)?)?;
    crate::expose(py, "pqbench.native.table", &module)?;
    crate::expose(py, "pqbench.native.table.get", &get)?;
    crate::expose(py, "pqbench.native.table.ls", &ls)?;
    Ok(())
}
