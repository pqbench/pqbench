//! `skill`: the skills compiled into the binary, and their documents.

use crate::marshal::{dumps, runtime};
use crate::prelude::*;

#[pyfunction]
fn skills(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let listed: Vec<Value> = pqbench::skill::skills()
        .iter()
        .map(|skill| {
            serde_json::json!({
                "name": skill.name,
                "description": skill.description,
                "documents": skill.documents.iter().map(|doc| doc.name).collect::<Vec<_>>(),
            })
        })
        .collect();
    dumps(py, &listed)
}
#[pyfunction]
fn skill(py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
    let skill = pqbench::skill::skill(name).map_err(runtime)?;
    dumps(
        py,
        &serde_json::json!({
            "name": skill.name,
            "description": skill.description,
            "documents": skill.documents.iter().map(|doc| doc.name).collect::<Vec<_>>(),
        }),
    )
}
#[pyfunction]
fn document(py: Python<'_>, name: &str, document: &str) -> PyResult<Py<PyAny>> {
    let text = pqbench::skill::document(name, document).map_err(runtime)?;
    Ok(text.into_pyobject(py)?.unbind().into_any())
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "skill")?;
    module.add_function(wrap_pyfunction!(skills, &module)?)?;
    module.add_function(wrap_pyfunction!(skill, &module)?)?;
    module.add_function(wrap_pyfunction!(document, &module)?)?;
    crate::expose(py, "pqbench.native.skill", &module)?;
    Ok(())
}
