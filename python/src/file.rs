//! `file`: list a partition's files, streamed.

use crate::handles::Lake;
use crate::marshal::stream_listing;
use crate::prelude::*;
use crate::records::TableFile;

#[pyfunction]
#[pyo3(name = "list")]
fn file_list(
    py: Python<'_>,
    lake: PyRef<'_, Lake>,
    uri: String,
    credentials: Option<Bound<'_, PyAny>>,
    versions: Vec<u64>,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    let credentials = crate::requests::credentials(credentials)?;
    stream_listing(
        py,
        async move { pqbench::file::list::list(&lake, &uri, credentials.as_ref(), &versions).await },
        TableFile::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "file")?;
    let list = crate::submodule(py, &module, "list")?;
    list.add_function(wrap_pyfunction!(file_list, &list)?)?;
    crate::expose(py, "pqbench.native.file", &module)?;
    crate::expose(py, "pqbench.native.file.list", &list)?;
    Ok(())
}
