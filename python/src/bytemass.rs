//! `bytemass`: measure the bytes of a file's columns.

use crate::handles::Lake;
use crate::prelude::*;
use crate::records::{many, MassRow};
use crate::requests::BytemassRequest;

#[pyfunction]
fn bytemass(
    py: Python<'_>,
    lake: PyRef<'_, Lake>,
    request: &BytemassRequest,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    let request = LibBytemassRequest::from(request);
    many(
        py,
        async move {
            pqbench::bytemass::bytemass(&lake, &request)
                .await
                .map_err(|error| error.to_string())
        },
        MassRow::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "bytemass")?;
    module.add_function(wrap_pyfunction!(bytemass, &module)?)?;
    crate::expose(py, "pqbench.native.bytemass", &module)?;
    Ok(())
}
