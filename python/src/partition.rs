//! `partition`: list a table's partitions, streamed.

use crate::handles::Lake;
use crate::marshal::stream_listing;
use crate::prelude::*;
use crate::records::Partition;

#[pyfunction]
#[pyo3(name = "list")]
fn partition_list(
    py: Python<'_>,
    lake: PyRef<'_, Lake>,
    table: String,
    uri: String,
    credentials: Option<Bound<'_, PyAny>>,
    window_millis: i64,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    let credentials = crate::requests::credentials(credentials)?;
    let env = credentials
        .as_ref()
        .map(pqbench::v1::Credentials::to_env)
        .unwrap_or_default();
    stream_listing(
        py,
        async move {
            let found =
                pqbench::partition::list::list(&lake, &table, &uri, &env, window_millis).await?;
            Ok(found.map_ok(move |mut partition| {
                partition.credentials = credentials.clone();
                partition
            }))
        },
        Partition::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "partition")?;
    let list = crate::submodule(py, &module, "list")?;
    list.add_function(wrap_pyfunction!(partition_list, &list)?)?;
    crate::expose(py, "pqbench.native.partition", &module)?;
    crate::expose(py, "pqbench.native.partition.list", &list)?;
    Ok(())
}
