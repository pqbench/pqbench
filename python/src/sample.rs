//! `third_party.parquet`: a parquet sample, and reading its rows.

use crate::marshal::runtime;
use crate::prelude::*;

/// A read sample for `pqbench.profile.profile`.
#[pyclass]
pub(crate) struct Sample(pub(crate) LibSample);

#[pymethods]
impl Sample {
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.0.columns.clone()
    }

    #[getter]
    fn row_count(&self) -> usize {
        self.0.rows.len()
    }
}
/// A typed sample for `pqbench.experiment.experiment`.
#[pyclass]
pub(crate) struct TypedSample(pub(crate) LibTypedSample);

#[pymethods]
impl TypedSample {
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.0
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    #[getter]
    fn row_count(&self) -> usize {
        self.0.rows.len()
    }
}
#[pyfunction]
#[pyo3(name = "read_sample")]
fn read_sample_rows(py: Python<'_>, path: PathBuf, max_rows: Option<usize>) -> PyResult<Py<PyAny>> {
    let sample = py
        .detach(|| read_sample(&path, max_rows))
        .map_err(runtime)?;
    Ok(Py::new(py, Sample(sample))?.into_any())
}
#[pyfunction]
#[pyo3(name = "read_typed_sample")]
fn read_typed_sample_rows(
    py: Python<'_>,
    path: PathBuf,
    max_rows: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let sample = py
        .detach(|| read_typed_sample(&path, max_rows))
        .map_err(runtime)?;
    Ok(Py::new(py, TypedSample(sample))?.into_any())
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<Sample>()?;
    root.add_class::<TypedSample>()?;
    let third_party = crate::submodule(py, root, "third_party")?;
    let parquet = crate::submodule(py, &third_party, "parquet")?;
    parquet.add_function(wrap_pyfunction!(read_sample_rows, &parquet)?)?;
    parquet.add_function(wrap_pyfunction!(read_typed_sample_rows, &parquet)?)?;
    crate::expose(py, "pqbench.native.third_party", &third_party)?;
    crate::expose(py, "pqbench.native.third_party.parquet", &parquet)?;
    Ok(())
}
