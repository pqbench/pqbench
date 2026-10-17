//! `compression`: compare codecs over one input.

use crate::marshal::{dumps, runtime};
use crate::prelude::*;
use crate::requests::CompressionRequest;

#[pyfunction]
fn compression(py: Python<'_>, request: &CompressionRequest) -> PyResult<Py<PyAny>> {
    let request = LibCompressionRequest::from(request);
    let report = py
        .detach(|| pqbench::compression::compression(&request))
        .map_err(runtime)?;
    dumps(py, &report)
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "compression")?;
    module.add_function(wrap_pyfunction!(compression, &module)?)?;
    crate::expose(py, "pqbench.native.compression", &module)?;
    Ok(())
}
