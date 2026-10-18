//! Marshalling: a library value to Python, and a stream as an iterator.

use crate::prelude::*;

/// The stream's channel: a built value, or the error that ended the stream.
pub(crate) type Channel = mpsc::Receiver<Result<Py<PyAny>, String>>;

/// A lazily-consumed stream of built values, driven on the shared runtime.
#[pyclass]
pub(crate) struct ValueIterator {
    rx: Arc<Mutex<Channel>>,
}
#[pymethods]
impl ValueIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = Arc::clone(&self.rx);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut receiver = rx.lock().await;
            match receiver.recv().await {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(Ok(value)) => Ok(value),
                Some(Err(error)) => Err(PyRuntimeError::new_err(error)),
            }
        })
    }
}
/// Drive a library stream on the shared runtime, as the Python async iterator.
pub(crate) fn stream_listing<F, S, T, E>(
    py: Python<'_>,
    source: F,
    attach: fn(Python<'_>, &T) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>>
where
    F: Future<Output = Result<S, E>> + Send + 'static,
    S: Stream<Item = Result<T, E>> + Send + 'static,
    T: Serialize + Send + 'static,
    E: std::fmt::Display + Send,
{
    let (tx, rx) = mpsc::channel(1);
    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        let stream = match source.await {
            Ok(stream) => stream,
            Err(error) => {
                let _ = tx.send(Err(error.to_string())).await;
                return;
            }
        };
        let mut stream = Box::pin(stream);
        while let Some(item) = stream.next().await {
            let value = match item {
                Ok(record) => {
                    Python::attach(|py| attach(py, &record)).map_err(|error| error.to_string())
                }
                Err(error) => Err(error.to_string()),
            };
            if tx.send(value).await.is_err() {
                return;
            }
        }
    });
    iterator(py, rx)
}
/// Wrap a value channel as the Python async iterator the streams return.
pub(crate) fn iterator(py: Python<'_>, rx: Channel) -> PyResult<Py<PyAny>> {
    Ok(Py::new(
        py,
        ValueIterator {
            rx: Arc::new(Mutex::new(rx)),
        },
    )?
    .into_any())
}
/// Drive a future on the shared runtime and return its awaitable.
pub(crate) fn drive(
    py: Python<'_>,
    future: impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
) -> PyResult<Py<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, future).map(Bound::unbind)
}
/// Drive a future whose value is a document, and return the awaitable.
pub(crate) fn document<T>(
    py: Python<'_>,
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> PyResult<Py<PyAny>>
where
    T: Serialize + Send + 'static,
{
    drive(py, async move {
        let value = future.await.map_err(PyRuntimeError::new_err)?;
        Python::attach(|py| dumps(py, &value))
    })
}
pub(crate) fn runtime(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
pub(crate) fn value_error(error: serde_json::Error) -> PyErr {
    PyValueError::new_err(error.to_string())
}
/// The error for an argument the library cannot read: the exception pyo3 raises
/// for an argument of the wrong type.
pub(crate) fn argument_error(error: impl ToString) -> PyErr {
    PyTypeError::new_err(error.to_string())
}
/// A Python value as JSON, via `json.dumps` — or a record, field for field.
pub(crate) fn py_to_value(object: &Bound<'_, PyAny>) -> PyResult<Value> {
    if let Ok(record) = object.extract::<PyRef<'_, crate::records::Record>>() {
        return Ok(Value::Object(record.fields().clone()));
    }
    let json = object.py().import("json")?;
    let text: String = json.call_method1("dumps", (object,))?.extract()?;
    serde_json::from_str(&text).map_err(argument_error)
}
pub(crate) fn dumps(py: Python<'_>, value: &impl Serialize) -> PyResult<Py<PyAny>> {
    let value = serde_json::to_value(value).map_err(value_error)?;
    value_to_py(py, &value)
}
pub(crate) fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    Ok(match value {
        Value::Null => py.None(),
        Value::Bool(value) => value.into_pyobject(py)?.to_owned().unbind().into_any(),
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                value.into_pyobject(py)?.unbind().into_any()
            } else if let Some(value) = number.as_u64() {
                value.into_pyobject(py)?.unbind().into_any()
            } else {
                number
                    .as_f64()
                    .ok_or_else(|| PyValueError::new_err("a number Python cannot represent"))?
                    .into_pyobject(py)?
                    .unbind()
                    .into_any()
            }
        }
        Value::String(text) => text.into_pyobject(py)?.unbind().into_any(),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(value_to_py(py, item)?)?;
            }
            list.unbind().into_any()
        }
        Value::Object(fields) => {
            let dict = PyDict::new(py);
            for (key, item) in fields {
                dict.set_item(key, value_to_py(py, item)?)?;
            }
            dict.unbind().into_any()
        }
    })
}

pub(crate) fn register(root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<ValueIterator>()?;
    Ok(())
}
