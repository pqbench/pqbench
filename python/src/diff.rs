//! `diff`: compare two bytemass records.

use crate::marshal::{dumps, py_records, rows, runtime};
use crate::prelude::*;

#[pyfunction]
#[pyo3(signature = (left, right, depth))]
fn compare(
    py: Python<'_>,
    left: Bound<'_, PyAny>,
    right: Bound<'_, PyAny>,
    depth: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let left = rows(&py_records(&left)?)?;
    let right = rows(&py_records(&right)?)?;
    let deltas = pqbench::diff::compare(&left, &right, depth).map_err(runtime)?;
    dumps(py, &deltas)
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "diff")?;
    module.add_function(wrap_pyfunction!(compare, &module)?)?;
    crate::expose(py, "pqbench.native.diff", &module)?;
    Ok(())
}
