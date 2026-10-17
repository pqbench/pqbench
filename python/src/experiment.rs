//! `experiment`: run the crate experiment, and its record.

use crate::marshal::{runtime, value_to_py};
use crate::prelude::*;
use crate::requests::ExperimentRequest;
use crate::sample::TypedSample;

#[pyfunction]
fn experiment(
    py: Python<'_>,
    sample: &TypedSample,
    request: &ExperimentRequest,
) -> PyResult<Py<PyAny>> {
    let request = LibExperimentRequest::from(request);
    let experiment = py
        .detach(|| pqbench::experiment::experiment(&sample.0, &request))
        .map_err(runtime)?;
    value_to_py(py, &experiment_value(&experiment))
}
/// The experiment result as the crate's record.
fn experiment_value(experiment: &pqbench::experiment::Experiment) -> Value {
    serde_json::json!({
        "row_count": experiment.row_count,
        "aim": experiment.aim.as_str(),
        "capabilities": experiment.capabilities.iter().map(|capability| serde_json::json!({
            "name": capability.name,
            "cost": capability.cost,
            "flag": capability.flag,
            "returns": capability.returns,
        })).collect::<Vec<_>>(),
        "trials": experiment.trials.iter().map(|trial| serde_json::json!({
            "name": trial.name,
            "bytes": trial.bytes,
            "bytes_per_row": trial.bytes_per_row,
            "row_group_count": trial.row_group_count,
            "ratio": trial.ratio,
            "columns": trial.columns.iter().map(|column| serde_json::json!({
                "column": column.column,
                "compressed_bytes": column.compressed_bytes,
                "bytes_per_row": column.bytes_per_row,
                "codec": column.codec,
                "encodings": column.encodings,
                "dictionary": column.dictionary,
                "skip_span_ratio": column.skip_span_ratio,
                "skip_point_equal_fraction": column.skip_point_equal_fraction,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    })
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "experiment")?;
    module.add_function(wrap_pyfunction!(experiment, &module)?)?;
    crate::expose(py, "pqbench.native.experiment", &module)?;
    Ok(())
}
