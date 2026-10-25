//! `schema`: read one schema, or list a catalog's.

use crate::handles::Workspace;
use crate::prelude::*;
use crate::records::{many, one, Schema};

#[pyfunction]
#[pyo3(name = "get")]
fn schema_get(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    name: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    let schema = pqbench::v1::Schema {
        name,
        ..Default::default()
    };
    one(
        py,
        async move {
            pqbench::schema::get::get(&workspace, &schema)
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
    let catalog = pqbench::v1::Catalog {
        name,
        ..Default::default()
    };
    many(
        py,
        async move {
            futures::TryStreamExt::try_collect::<Vec<_>>(pqbench::schema::list::list(
                &workspace, &catalog,
            ))
            .await
            .map_err(|error| error.to_string())
        },
        Schema::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "schema")?;
    let get = crate::submodule(py, &module, "get")?;
    get.add_function(wrap_pyfunction!(schema_get, &get)?)?;
    let list = crate::submodule(py, &module, "list")?;
    list.add_function(wrap_pyfunction!(schema_list, &list)?)?;
    crate::expose(py, "pqbench.native.schema", &module)?;
    crate::expose(py, "pqbench.native.schema.get", &get)?;
    crate::expose(py, "pqbench.native.schema.list", &list)?;
    Ok(())
}
