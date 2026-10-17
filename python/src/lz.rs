//! `lz`: measure the compressors on one input.

use crate::marshal::{dumps, runtime};
use crate::prelude::*;
use crate::requests::LzRequest;

#[pyfunction]
fn lz(py: Python<'_>, request: &LzRequest) -> PyResult<Py<PyAny>> {
    let request = LibLzRequest::from(request);
    let report = py.detach(|| pqbench::lz::lz(&request)).map_err(runtime)?;
    dumps(py, &report)
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "lz")?;
    module.add_function(wrap_pyfunction!(lz, &module)?)?;
    crate::expose(py, "pqbench.native.lz", &module)?;
    Ok(())
}
