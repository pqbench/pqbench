//! `catalog`: read one catalog, or list them all.

use crate::handles::Workspace;
use crate::prelude::*;
use crate::records::{many, one, Catalog};

#[pyfunction]
#[pyo3(name = "get")]
fn catalog_get(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    name: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    let catalog = pqbench::v1::Catalog {
        name,
        ..Default::default()
    };
    one(
        py,
        async move {
            pqbench::catalog::get::get(&workspace, &catalog)
                .await
                .map_err(|error| error.to_string())
        },
        Catalog::attach,
    )
}
#[pyfunction]
#[pyo3(name = "list")]
fn catalog_list(py: Python<'_>, workspace: PyRef<'_, Workspace>) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    many(
        py,
        async move {
            futures::TryStreamExt::try_collect::<Vec<_>>(pqbench::catalog::list::list(&workspace))
                .await
                .map_err(|error| error.to_string())
        },
        Catalog::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "catalog")?;
    let get = crate::submodule(py, &module, "get")?;
    get.add_function(wrap_pyfunction!(catalog_get, &get)?)?;
    let list = crate::submodule(py, &module, "list")?;
    list.add_function(wrap_pyfunction!(catalog_list, &list)?)?;
    crate::expose(py, "pqbench.native.catalog", &module)?;
    crate::expose(py, "pqbench.native.catalog.get", &get)?;
    crate::expose(py, "pqbench.native.catalog.list", &list)?;
    Ok(())
}
