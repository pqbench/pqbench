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
    env: BTreeMap<String, String>,
    window_millis: i64,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    stream_listing(
        py,
        async move { pqbench::partition::ls::list(&lake, &table, &uri, &env, window_millis).await },
        Partition::attach,
    )
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "partition")?;
    let ls = crate::submodule(py, &module, "ls")?;
    ls.add_function(wrap_pyfunction!(partition_list, &ls)?)?;
    crate::expose(py, "pqbench.native.partition", &module)?;
    crate::expose(py, "pqbench.native.partition.ls", &ls)?;
    Ok(())
}
