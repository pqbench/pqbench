//! `partition`: list a table's partitions, streamed.

use crate::handles::Lake;
use crate::marshal::stream_listing;
use crate::prelude::*;
use crate::records::Partition;

#[pyfunction]
#[pyo3(name = "list")]
#[pyo3(signature = (lake, table, uri, credentials, window_millis, last_millis=None))]
fn partition_list(
    py: Python<'_>,
    lake: PyRef<'_, Lake>,
    table: String,
    uri: String,
    credentials: Option<Bound<'_, PyAny>>,
    window_millis: i64,
    last_millis: Option<i64>,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    let credentials = crate::requests::credentials(credentials)?;
    let table = pqbench::v1::Table {
        name: table,
        uri: Some(uri),
        credentials: credentials.clone(),
        ..pqbench::v1::Table::default()
    };
    stream_listing(
        py,
        async move {
            let found =
                pqbench::partition::list::list(&lake, &table, window_millis, last_millis).await?;
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
