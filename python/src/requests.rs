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
        .map_err(crate::marshal::value_error)
}
/// Arguments for `pqbench.bytemass.bytemass`.
#[pyclass(skip_from_py_object)]
#[derive(Clone, Default)]
pub(crate) struct BytemassRequest {
    inputs: Vec<String>,
    env: BTreeMap<String, String>,
    indexes: bool,
}
#[pymethods]
impl BytemassRequest {
    #[new]
    #[pyo3(signature = (inputs, env=None, indexes=false))]
    fn new(inputs: Vec<String>, env: Option<BTreeMap<String, String>>, indexes: bool) -> Self {
        Self {
            inputs,
            env: env.unwrap_or_default(),
            indexes,
        }
    }
}
impl From<&BytemassRequest> for LibBytemassRequest {
    fn from(value: &BytemassRequest) -> Self {
        Self {
            inputs: value.inputs.clone(),
            env: value.env.clone(),
            indexes: value.indexes,
        }
    }
}
pub(crate) fn register(root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<BytemassRequest>()?;
    Ok(())
}
