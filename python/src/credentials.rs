//! `credentials`: whether a table is readable here, and its lease.

use crate::handles::Workspace;
use crate::prelude::*;
use crate::records::{one, Credentials, ReadEligibility};

#[pyfunction]
#[pyo3(name = "check")]
fn credentials_check(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    name: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    one(
        py,
        async move {
            pqbench::credentials::check::check(&workspace, &name)
                .await
                .map_err(|error| error.to_string())
        },
        ReadEligibility::attach,
    )
}
/// Vend the table `name`'s read credentials: `name` -> lease.
#[pyfunction]
#[pyo3(name = "get")]
fn credentials_get(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    name: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    one(
        py,
        async move {
            pqbench::credentials::get::get(&workspace, &name)
                .await
                .map_err(|error| error.to_string())
        },
        Credentials::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let credentials = crate::submodule(py, root, "credentials")?;
    let check = crate::submodule(py, &credentials, "check")?;
    check.add_function(wrap_pyfunction!(credentials_check, &check)?)?;
    let get = crate::submodule(py, &credentials, "get")?;
    get.add_function(wrap_pyfunction!(credentials_get, &get)?)?;
    crate::expose(py, "pqbench.native.credentials", &credentials)?;
    crate::expose(py, "pqbench.native.credentials.check", &check)?;
    crate::expose(py, "pqbench.native.credentials.get", &get)?;
    Ok(())
}
