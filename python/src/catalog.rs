//! `catalog`: read one catalog, or list them all.

use crate::handles::Workspace;
use crate::marshal::records;
use crate::prelude::*;

#[pyfunction]
#[pyo3(name = "read")]
fn catalog_read(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::catalog::get::read(&workspace, &catalog)
            .await
            .map_err(|error| error.to_string())
    })
}
#[pyfunction]
#[pyo3(name = "list")]
fn catalog_list(py: Python<'_>, workspace: PyRef<'_, Workspace>) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::catalog::ls::list(&workspace)
            .await
            .map_err(|error| error.to_string())
    })
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "catalog")?;
    let get = crate::submodule(py, &module, "get")?;
    get.add_function(wrap_pyfunction!(catalog_read, &get)?)?;
    let ls = crate::submodule(py, &module, "ls")?;
    ls.add_function(wrap_pyfunction!(catalog_list, &ls)?)?;
    crate::expose(py, "pqbench.native.catalog", &module)?;
    crate::expose(py, "pqbench.native.catalog.get", &get)?;
    crate::expose(py, "pqbench.native.catalog.ls", &ls)?;
    Ok(())
}
