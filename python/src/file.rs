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
    env: BTreeMap<String, String>,
    versions: Vec<u64>,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    stream_listing(
        py,
        async move { pqbench::file::ls::list(&lake, &uri, &env, &versions).await },
        TableFile::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "file")?;
    let ls = crate::submodule(py, &module, "ls")?;
    ls.add_function(wrap_pyfunction!(file_list, &ls)?)?;
    crate::expose(py, "pqbench.native.file", &module)?;
    crate::expose(py, "pqbench.native.file.ls", &ls)?;
    Ok(())
}
