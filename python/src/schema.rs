//! `schema`: read one schema, or list a catalog's.

use crate::handles::Workspace;
use crate::prelude::*;
use crate::records::{many, one, Schema};

#[pyfunction]
#[pyo3(name = "read")]
fn schema_read(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    name: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    one(
        py,
        async move {
            pqbench::schema::get::read(&workspace, &name)
                .await
                .map_err(|error| error.to_string())
        },
        Schema::attach,
    )
}
#[pyfunction]
#[pyo3(name = "list")]
fn schema_list(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    name: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    many(
        py,
        async move {
            pqbench::schema::list::list(&workspace, &name)
                .await
                .map_err(|error| error.to_string())
        },
        Schema::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "schema")?;
    let get = crate::submodule(py, &module, "get")?;
    get.add_function(wrap_pyfunction!(schema_read, &get)?)?;
    let list = crate::submodule(py, &module, "list")?;
    list.add_function(wrap_pyfunction!(schema_list, &list)?)?;
    crate::expose(py, "pqbench.native.schema", &module)?;
    crate::expose(py, "pqbench.native.schema.get", &get)?;
    crate::expose(py, "pqbench.native.schema.list", &list)?;
    Ok(())
}
