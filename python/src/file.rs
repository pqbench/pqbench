//! `file`: a table's active files, streamed.

use crate::handles::Lake;
use crate::marshal::stream_listing;
use crate::prelude::*;
use crate::records::TableFile;

#[pyfunction]
#[pyo3(name = "list")]
#[pyo3(signature = (lake, table, uri, credentials, partition_millis=None, last_millis=None))]
fn file_list(
    py: Python<'_>,
    lake: PyRef<'_, Lake>,
    table: String,
    uri: String,
    credentials: Option<Bound<'_, PyAny>>,
    partition_millis: Option<i64>,
    last_millis: Option<i64>,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    let credentials = crate::requests::credentials(credentials)?;
    let record = pqbench::v1::Table {
        name: table,
        uri: Some(uri),
        credentials: credentials.clone(),
        ..pqbench::v1::Table::default()
    };
    let window = crate::requests::window(partition_millis, last_millis)?;
    stream_listing(
        py,
        async move {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let producer = sender.clone();
            tokio::spawn(async move {
                let mut producer = producer;
                let walked = pqbench::file::list::visit(
                    &lake,
                    &record,
                    window,
                    &mut |file| producer.send(Ok(file)).is_ok(),
                )
                .await;
                if let Err(error) = walked {
                    let _ = sender.send(Err(error.to_string()));
                }
            });
            Ok(futures::stream::unfold(receiver, |mut receiver| async move {
                receiver.recv().await.map(|item| (item, receiver))
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
