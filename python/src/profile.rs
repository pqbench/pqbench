//! `profile`: profile a sample against a request.

use crate::marshal::{dumps, runtime};
use crate::prelude::*;
use crate::requests::ProfileRequest;
use crate::sample::Sample;

#[pyfunction]
fn profile(py: Python<'_>, sample: &Sample, request: &ProfileRequest) -> PyResult<Py<PyAny>> {
    let request = LibProfileRequest::from(request);
    let profile = py
        .detach(|| pqbench::profile::profile(&sample.0, &request))
        .map_err(runtime)?;
    dumps(py, &profile)
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "profile")?;
    module.add_function(wrap_pyfunction!(profile, &module)?)?;
    crate::expose(py, "pqbench.native.profile", &module)?;
    Ok(())
}
