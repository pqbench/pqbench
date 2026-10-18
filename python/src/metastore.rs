//! `metastore`: the whole metastore, as the crate record.

use crate::handles::Workspace;
use crate::marshal::document;
use crate::prelude::*;

#[pyfunction]
#[pyo3(name = "get")]
fn metastore_get(py: Python<'_>, workspace: PyRef<'_, Workspace>) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    document(py, async move {
        pqbench::metastore::get::get(&workspace)
            .await
            .map_err(|error| error.to_string())
    })
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let metastore = crate::submodule(py, root, "metastore")?;
    let get = crate::submodule(py, &metastore, "get")?;
    get.add_function(wrap_pyfunction!(metastore_get, &get)?)?;
    crate::expose(py, "pqbench.native.metastore", &metastore)?;
    crate::expose(py, "pqbench.native.metastore.get", &get)?;
    Ok(())
}
