//! `viz`: a bytemass stream as the crate treemap record.

use crate::marshal::{py_records, runtime, value_error};
use crate::prelude::*;

#[pyfunction]
#[pyo3(signature = (prefix, rows, files=None))]
fn write_report(
    py: Python<'_>,
    prefix: PathBuf,
    rows: Bound<'_, PyAny>,
    files: Option<Bound<'_, PyAny>>,
) -> PyResult<()> {
    let records: Vec<MassRecord> = py_records(&rows)?.iter().map(mass_record).collect();
    let files: Vec<pqbench::bytemass::FileStat> = match files {
        Some(files) => py_records(&files)?
            .iter()
            .map(|value| serde_json::from_value(value.clone()).map_err(value_error))
            .collect::<PyResult<_>>()?,
        None => Vec::new(),
    };
    py.detach(|| pqbench::viz::write_report(&prefix, &records, &files))
        .map_err(runtime)
}
/// One HTML-treemap record from a bytemass row.
fn mass_record(value: &Value) -> MassRecord {
    let text = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let number = |key: &str| value.get(key).and_then(Value::as_u64).unwrap_or_default();
    MassRecord {
        id: text("uri"),
        file: text("uri"),
        column: text("column"),
        codec: text("codec"),
        physical_type: text("physical_type"),
        encodings: value.get("encodings").map(encodings).unwrap_or_default(),
        size: number("size_bytes"),
        row_count: number("row_count"),
        compressed_bytes: number("compressed_bytes"),
        uncompressed_bytes: number("uncompressed_bytes"),
        num_values: number("num_values"),
        dictionary: value
            .get("dictionary")
            .and_then(Value::as_bool)
            .unwrap_or_default(),
        null_count: value.get("null_count").and_then(Value::as_u64),
        distinct_count: value.get("distinct_count").and_then(Value::as_u64),
        row_group: value
            .get("row_group")
            .and_then(Value::as_u64)
            .unwrap_or_default() as u32,
        row_group_rows: number("row_group_rows"),
        compressed_bytes_per_row: value
            .get("compressed_bytes_per_row")
            .and_then(Value::as_f64),
        page_count: value.get("page_count").and_then(Value::as_u64),
    }
}
/// Encodings as the page expects them: comma-joined.
fn encodings(value: &Value) -> String {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    let module = crate::submodule(py, root, "viz")?;
    module.add_function(wrap_pyfunction!(write_report, &module)?)?;
    crate::expose(py, "pqbench.native.viz", &module)?;
    Ok(())
}
