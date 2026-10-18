//! The records a command returns: the library's `Serialize` models, as classes.
//!
//! A record is its model's fields under a class that names it, so
//! `type(file).__name__ == "TableFile"`, `file["uri"]`, `file.uri` and
//! `file.asdict()` all agree, and `isinstance(file, TableFile)` says what it is.
//! The structs are `#[non_exhaustive]`, so a field the library adds is a key
//! here without a binding change; the deep trees they embed (`FileStats`, the
//! parquet metadata, a catalog's schemas) stay dicts.
//!
//! A record is mutable in one place: a composed stage puts the lease that read
//! it on the record, the way the CLI's `table-file` record carries
//! `credentials` to `bytemass`.

use crate::marshal::{drive, value_error, value_to_py};
use crate::prelude::*;
use pyo3::exceptions::{PyAttributeError, PyKeyError};

/// One record of a library model, its fields, and the mapping interface.
#[pyclass(subclass, eq, module = "pqbench.native")]
#[derive(PartialEq)]
pub(crate) struct Record {
    fields: serde_json::Map<String, Value>,
    model: &'static str,
}

#[pymethods]
impl Record {
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match self.fields.get(key) {
            Some(value) => value_to_py(py, value),
            None => Err(PyKeyError::new_err(key.to_string())),
        }
    }

    fn __setitem__(&mut self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        self.fields
            .insert(key.to_string(), crate::marshal::py_to_value(value)?);
        Ok(())
    }

    fn __contains__(&self, key: &str) -> bool {
        self.fields.contains_key(key)
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.fields.keys())?
            .try_iter()?
            .into_any()
            .unbind())
    }

    fn __len__(&self) -> usize {
        self.fields.len()
    }

    /// The field names, in the model's order.
    fn keys(&self) -> Vec<String> {
        self.fields.keys().cloned().collect()
    }

    fn values(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.fields
            .values()
            .map(|value| value_to_py(py, value))
            .collect()
    }

    fn items(&self, py: Python<'_>) -> PyResult<Vec<(String, Py<PyAny>)>> {
        self.fields
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_to_py(py, value)?)))
            .collect()
    }

    /// The field, or `default` when the model has none.
    #[pyo3(signature = (key, default = None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        match self.fields.get(key) {
            Some(value) => value_to_py(py, value),
            None => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    /// The record as a dict, the shape a JSON command emits.
    fn asdict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        value_to_py(py, &Value::Object(self.fields.clone()))
    }

    fn __getattr__(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        match self.fields.get(name) {
            Some(value) => value_to_py(py, value),
            None => Err(PyAttributeError::new_err(format!(
                "{} has no field {name:?}",
                self.model
            ))),
        }
    }

    fn __repr__(&self) -> String {
        let fields = self
            .fields
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}({fields})", self.model)
    }
}

impl Record {
    /// The record's fields, for the binding's own conversions.
    pub(crate) fn fields(&self) -> &serde_json::Map<String, Value> {
        &self.fields
    }
}

/// A model's record class: the base carries the fields, the class names it.
macro_rules! record_class {
    ($($name:ident),* $(,)?) => {$(
        #[pyclass(extends = Record, module = "pqbench.native")]
        pub(crate) struct $name;

        impl $name {
            /// This record, from one value of the model.
            pub(crate) fn attach<T: Serialize>(
                py: Python<'_>,
                value: &T,
            ) -> PyResult<Py<PyAny>> {
                let fields = match serde_json::to_value(value).map_err(value_error)? {
                    Value::Object(fields) => fields,
                    other => return value_to_py(py, &other),
                };
                let init = PyClassInitializer::from(Record {
                    fields,
                    model: stringify!($name),
                })
                .add_subclass($name);
                Ok(Py::new(py, init)?.into_any())
            }
        }
    )*};
}

record_class!(
    Catalog,
    Schema,
    TableRef,
    Table,
    Partition,
    TableFile,
    MassRow,
    Credentials
);

/// The function that wraps one value of a model as its record.
pub(crate) type Attach<T> = fn(Python<'_>, &T) -> PyResult<Py<PyAny>>;

/// One record, from a command whose library call returns one value.
pub(crate) fn one<F, T>(py: Python<'_>, future: F, attach: Attach<T>) -> PyResult<Py<PyAny>>
where
    F: Future<Output = Result<T, String>> + Send + 'static,
    T: Serialize + Send + 'static,
{
    drive(py, async move {
        let value = future.await.map_err(PyRuntimeError::new_err)?;
        Python::attach(|py| attach(py, &value))
    })
}

/// A list of records, from a command whose library call returns a vector.
pub(crate) fn many<F, T>(py: Python<'_>, future: F, attach: Attach<T>) -> PyResult<Py<PyAny>>
where
    F: Future<Output = Result<Vec<T>, String>> + Send + 'static,
    T: Serialize + Send + 'static,
{
    drive(py, async move {
        let values = future.await.map_err(PyRuntimeError::new_err)?;
        Python::attach(|py| {
            let list = PyList::empty(py);
            for value in &values {
                list.append(attach(py, value)?)?;
            }
            Ok(list.unbind().into_any())
        })
    })
}

/// Expose the base and every model's class.
pub(crate) fn register(root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<Record>()?;
    root.add_class::<Catalog>()?;
    root.add_class::<Schema>()?;
    root.add_class::<TableRef>()?;
    root.add_class::<Table>()?;
    root.add_class::<Partition>()?;
    root.add_class::<TableFile>()?;
    root.add_class::<MassRow>()?;
    root.add_class::<Credentials>()?;
    Ok(())
}
