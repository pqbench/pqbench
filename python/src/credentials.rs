//! `credentials`: whether a table is readable here, and its lease.

use crate::handles::Workspace;
use crate::marshal::drive;
use crate::prelude::*;
use crate::records::{one, Credentials};

#[pyclass]
pub(crate) struct Eligibility {
    eligible: bool,
    reason: Option<String>,
}
#[pymethods]
impl Eligibility {
    #[getter]
    fn eligible(&self) -> bool {
        self.eligible
    }

    #[getter]
    fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    fn __repr__(&self) -> String {
        match &self.reason {
            Some(reason) => format!("Eligibility(eligible=False, reason={reason:?})"),
            None => "Eligibility(eligible=True)".to_string(),
        }
    }
}
impl From<pqbench::credentials::check::Eligibility> for Eligibility {
    fn from(value: pqbench::credentials::check::Eligibility) -> Self {
        use pqbench::credentials::check::Eligibility as Lib;
        match value {
            Lib::Eligible => Self {
                eligible: true,
                reason: None,
            },
            Lib::Ineligible(reason) => Self {
                eligible: false,
                reason: Some(
                    match reason {
                        pqbench::credentials::check::Reason::SystemTable => "system-table",
                        pqbench::credentials::check::Reason::NotATable => "not-a-table",
                        pqbench::credentials::check::Reason::NoExternalRead => "no-external-read",
                    }
                    .to_string(),
                ),
            },
        }
    }
}
#[pyfunction]
#[pyo3(name = "check")]
fn credentials_check(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    name: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    drive(py, async move {
        let eligibility = pqbench::credentials::check::check(&workspace, &name)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Python::attach(|py| Ok(Py::new(py, Eligibility::from(eligibility))?.into_any()))
    })
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
    root.add_class::<Eligibility>()?;
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
