//! `schema`: read one schema, or list a catalog's.

use crate::handles::Workspace;
use crate::marshal::records;
use crate::prelude::*;

#[pyfunction]
#[pyo3(name = "read")]
fn schema_read(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
    schema: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::schema::get::read(&workspace, &catalog, &schema)
            .await
            .map_err(|error| error.to_string())
    })
}
#[pyfunction]
#[pyo3(name = "list")]
fn schema_list(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::schema::ls::list(&workspace, &catalog)
            .await
            .map_err(|error| error.to_string())
    })
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "schema")?;
    let get = crate::submodule(py, &module, "get")?;
    get.add_function(wrap_pyfunction!(schema_read, &get)?)?;
    let ls = crate::submodule(py, &module, "ls")?;
    ls.add_function(wrap_pyfunction!(schema_list, &ls)?)?;
    crate::expose(py, "pqbench.native.schema", &module)?;
    crate::expose(py, "pqbench.native.schema.get", &get)?;
    crate::expose(py, "pqbench.native.schema.ls", &ls)?;
    Ok(())
}
