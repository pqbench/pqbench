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
    table: String,
    uri: String,
    credentials: Option<Bound<'_, PyAny>>,
    versions: Vec<u64>,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    let credentials = crate::requests::credentials(credentials)?;
    let partition = pqbench::v1::Partition {
        name: pqbench::v1::name::partition_name(&table, "0"),
        uri: Some(uri),
        commit_versions: versions,
        credentials: credentials.clone(),
        ..pqbench::v1::Partition::default()
    };
    stream_listing(
        py,
        async move {
            let found = pqbench::file::list::list(&lake, &partition).await?;
            Ok(found.map_ok(move |mut file| {
                file.credentials = credentials.clone();
                file
            }))
        },
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
