//! The typed requests: what a caller passes to a command.

use crate::prelude::*;

/// The vended lease a command reads with: a `Credentials` record, or the
/// mapping of the same fields, as the model's credentials.
pub(crate) fn credentials(
    value: Option<Bound<'_, PyAny>>,
) -> PyResult<Option<pqbench::v1::Credentials>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = crate::marshal::py_to_value(&value)?;
    serde_json::from_value(value)
        .map(Some)
        .map_err(crate::marshal::argument_error)
}
/// The window a `file.list` call narrows to: one UTC day, or the last
/// `last_millis`, ending now. Both set is an error.
pub(crate) fn window(
    partition_millis: Option<i64>,
    last_millis: Option<i64>,
) -> PyResult<Option<pqbench::time::TimeWindow>> {
    match (partition_millis, last_millis) {
        (Some(_), Some(_)) => Err(pyo3::exceptions::PyValueError::new_err(
            "pass a partition day or a last window, not both",
        )),
        (Some(day), None) => {
            let day = u64::try_from(day)
                .map_err(|_| pyo3::exceptions::PyValueError::new_err("partition_millis is negative"))?;
            Ok(Some(pqbench::time::TimeWindow::day(day)))
        }
        (None, Some(last)) => {
            let last = u64::try_from(last)
                .map_err(|_| pyo3::exceptions::PyValueError::new_err("last_millis is negative"))?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            Ok(Some(pqbench::time::TimeWindow {
                first_time: now.saturating_sub(last),
                last_time: now,
            }))
        }
        (None, None) => Ok(None),
    }
}

/// Arguments for `pqbench.bytemass.bytemass`.
#[pyclass(skip_from_py_object)]
#[derive(Clone, Default)]
pub(crate) struct BytemassRequest {
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
}
#[pymethods]
impl BytemassRequest {
    #[new]
    #[pyo3(signature = (inputs, env=None))]
    fn new(inputs: Vec<String>, env: Option<BTreeMap<String, String>>) -> Self {
        Self {
            inputs,
            env: env.unwrap_or_default(),
        }
    }
}
impl From<&BytemassRequest> for LibBytemassRequest {
    fn from(value: &BytemassRequest) -> Self {
        Self {
            inputs: value.inputs.clone(),
            env: value.env.clone(),
        }
    }
}
pub(crate) fn register(root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<BytemassRequest>()?;
    Ok(())
}
