//! In-process bindings for the pqbench commands.
//!
//! Each function calls the same library entry point as the matching CLI
//! subcommand and returns the same document, bytes, or paths the CLI would
//! produce.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use pqbench::dump::DumpFile;
use pqbench::lake::Lake;
use pqbench::stats;
use pqbench::table::TableInfo;
use pqbench::viz::MassRecord;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::{Py, PyAny};
use serde::Serialize;
use serde_json::Value;

/// Run `pqbench lz` over the raw bytes of `file`.
///
/// `codecs` is repeatable `codec@level`. `mode` is `fastest` or `mean`.
/// `json=True` returns the report object; otherwise the text table.
#[pyfunction]
#[pyo3(signature = (file, *, codecs=None, samples=10, warmup_iterations=3, mode="fastest", json=false))]
fn lz(
    py: Python<'_>,
    file: PathBuf,
    codecs: Option<Vec<String>>,
    samples: u32,
    warmup_iterations: u32,
    mode: &str,
    json: bool,
) -> PyResult<Py<PyAny>> {
    let request = pqbench::lz::LzRequest {
        file,
        codec_specs: codecs.unwrap_or_default(),
        samples,
        warmup_iterations,
        mode: parse_mode(mode)?,
    };
    let report = py.detach(|| pqbench::lz::lz(&request)).map_err(runtime)?;
    if json {
        to_object(py, &pqbench::lz::render_json(&report).map_err(runtime)?)
    } else {
        as_text(py, pqbench::lz::render_text(&report).map_err(runtime)?)
    }
}

/// Run `pqbench compression` over a NONE-compressed parquet file.
///
/// Same sweep arguments as `lz`, plus `per_column`. `json=True` returns the
/// report object.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (
    file,
    *,
    codecs=None,
    samples=10,
    warmup_iterations=3,
    mode="fastest",
    per_column=false,
    json=false
))]
fn compression(
    py: Python<'_>,
    file: PathBuf,
    codecs: Option<Vec<String>>,
    samples: u32,
    warmup_iterations: u32,
    mode: &str,
    per_column: bool,
    json: bool,
) -> PyResult<Py<PyAny>> {
    let request = pqbench::compression::CompressionRequest {
        file,
        codec_specs: codecs.unwrap_or_default(),
        samples,
        warmup_iterations,
        mode: parse_mode(mode)?,
        per_column,
    };
    let report = py
        .detach(|| pqbench::compression::compression(&request))
        .map_err(runtime)?;
    if json {
        to_object(
            py,
            &pqbench::compression::render_json(&report).map_err(runtime)?,
        )
    } else {
        as_text(
            py,
            pqbench::compression::render_text(&report, per_column).map_err(runtime)?,
        )
    }
}

/// Run `pqbench bytemass` on parquet paths or glob masks.
///
/// Returns the stream of `pqbench.bytemass-row` objects (one dict per column
/// chunk).
#[pyfunction]
#[pyo3(signature = (*inputs, env=None))]
fn bytemass(
    py: Python<'_>,
    inputs: Vec<String>,
    env: Option<BTreeMap<String, String>>,
) -> PyResult<Py<PyAny>> {
    let request = pqbench::bytemass::BytemassRequest {
        inputs,
        env: aws_env(env)?,
    };
    let rows = py
        .detach(|| block_on(pqbench::bytemass::bytemass(&request)))
        .map_err(runtime)?;
    dumps(py, &rows)
}

/// Run `pqbench table`: detect the format and load one snapshot.
///
/// Returns the `pqbench.table` document.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (
    uri,
    *,
    version=None,
    env=None,
    exclude_modified_before=None,
    exclude_modified_after=None,
    exclude_version_before=None,
    exclude_version_after=None,
    exclude_snapshot_before=None,
    exclude_snapshot_after=None
))]
fn table(
    py: Python<'_>,
    uri: String,
    version: Option<u64>,
    env: Option<BTreeMap<String, String>>,
    exclude_modified_before: Option<String>,
    exclude_modified_after: Option<String>,
    exclude_version_before: Option<u64>,
    exclude_version_after: Option<u64>,
    exclude_snapshot_before: Option<String>,
    exclude_snapshot_after: Option<String>,
) -> PyResult<Py<PyAny>> {
    let selection = pqbench::pattern::Selection {
        exclude_modified_before,
        exclude_modified_after,
        exclude_version_before,
        exclude_version_after,
        exclude_snapshot_before,
        exclude_snapshot_after,
        ..pqbench::pattern::Selection::default()
    };
    for time in [
        &selection.exclude_modified_before,
        &selection.exclude_modified_after,
        &selection.exclude_snapshot_before,
        &selection.exclude_snapshot_after,
    ]
    .into_iter()
    .flatten()
    {
        pqbench::table::validate_time(time).map_err(|e| PyValueError::new_err(e.to_string()))?;
    }
    let request =
        pqbench::table::LoadRequest::new(uri, version, aws_env(env)?).with_selection(selection);
    let info = py
        .detach(|| block_on(pqbench::table::load(&request)))
        .map_err(runtime)?;
    dumps(py, &info)
}

/// Run `pqbench lake`: list tables under a directory.
///
/// Returns the `pqbench.lake` document.
#[pyfunction]
#[pyo3(signature = (root, *, max_depth=None))]
fn lake(py: Python<'_>, root: PathBuf, max_depth: Option<usize>) -> PyResult<Py<PyAny>> {
    let lake = py
        .detach(move || pqbench::lake::discover_at(Path::new(&root), max_depth))
        .map_err(runtime)?;
    dumps(py, &lake)
}

/// Run `pqbench dump` on parquet paths or a `pqbench.table` / `pqbench.lake`
/// document.
///
/// Returns Parquet bytes.
#[pyfunction]
#[pyo3(signature = (*inputs, row_groups="all"))]
fn dump(py: Python<'_>, inputs: Vec<Bound<'_, PyAny>>, row_groups: &str) -> PyResult<Py<PyAny>> {
    let files = dump_files(&inputs)?;
    let request = pqbench::dump::DumpRequest {
        files,
        row_groups: pqbench::dump::RowGroups::parse(row_groups).map_err(runtime)?,
    };
    let bytes = py
        .detach(|| block_on(pqbench::dump::write_parquet(&request)))
        .map_err(runtime)?;
    Ok(PyBytes::new(py, &bytes).into_any().unbind())
}

/// Run `pqbench viz` on a bytemass row stream.
///
/// Writes `output.sqlite` and `output.html`. Returns those paths.
#[pyfunction]
#[pyo3(signature = (rows, *, output))]
fn viz(py: Python<'_>, rows: Bound<'_, PyAny>, output: PathBuf) -> PyResult<Py<PyAny>> {
    let records = viz_rows(&rows)?;
    let prefix = match output.extension().and_then(|ext| ext.to_str()) {
        Some("html" | "htm" | "sqlite" | "db") => output.with_extension(""),
        _ => output,
    };
    py.detach(|| pqbench::viz::write_report(&prefix, &records))
        .map_err(runtime)?;
    let mut result = serde_json::Map::new();
    result.insert(
        "sqlite".into(),
        Value::String(prefix.with_extension("sqlite").display().to_string()),
    );
    result.insert(
        "html".into(),
        Value::String(prefix.with_extension("html").display().to_string()),
    );
    value_to_py(py, &Value::Object(result))
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(lz, module)?)?;
    module.add_function(wrap_pyfunction!(compression, module)?)?;
    module.add_function(wrap_pyfunction!(bytemass, module)?)?;
    module.add_function(wrap_pyfunction!(table, module)?)?;
    module.add_function(wrap_pyfunction!(lake, module)?)?;
    module.add_function(wrap_pyfunction!(dump, module)?)?;
    module.add_function(wrap_pyfunction!(viz, module)?)?;
    module.add(
        "commands",
        PyTuple::new(
            module.py(),
            [
                "lz",
                "compression",
                "bytemass",
                "table",
                "lake",
                "dump",
                "viz",
            ],
        )?,
    )?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

fn parse_mode(mode: &str) -> PyResult<stats::Mode> {
    match mode {
        "fastest" => Ok(stats::Mode::Fastest),
        "mean" => Ok(stats::Mode::Mean),
        other => Err(PyValueError::new_err(format!(
            "invalid mode {other:?}; expected fastest or mean"
        ))),
    }
}

fn aws_env(env: Option<BTreeMap<String, String>>) -> PyResult<BTreeMap<String, String>> {
    let env = env.unwrap_or_default();
    if let Some(key) = env.keys().find(|key| !key.starts_with("AWS_")) {
        return Err(PyValueError::new_err(format!(
            "env may only contain AWS_* names, not `{key}`"
        )));
    }
    Ok(env)
}

fn dump_files(inputs: &[Bound<'_, PyAny>]) -> PyResult<Vec<DumpFile>> {
    if inputs.len() == 1 {
        if let Ok(value) = py_to_value(&inputs[0]) {
            match value.get("kind").and_then(|kind| kind.as_str()) {
                Some("pqbench.table") => {
                    let info: TableInfo = serde_json::from_value(value).map_err(runtime)?;
                    return Ok(files_from_table(&info, None));
                }
                Some("pqbench.lake") => {
                    let lake: Lake = serde_json::from_value(value).map_err(runtime)?;
                    return files_from_lake(&lake);
                }
                _ => {}
            }
        }
    }
    inputs
        .iter()
        .map(|input| {
            let path = input.extract::<String>()?;
            Ok(DumpFile {
                path: path.clone(),
                uri: path,
                table: None,
                env: BTreeMap::new(),
            })
        })
        .collect()
}

fn files_from_table(info: &TableInfo, table: Option<&str>) -> Vec<DumpFile> {
    info.files
        .iter()
        .map(|file| DumpFile {
            path: file.path.clone(),
            uri: file.uri.clone(),
            table: table.map(str::to_string),
            env: info.env.clone(),
        })
        .collect()
}

fn files_from_lake(lake: &Lake) -> PyResult<Vec<DumpFile>> {
    let mut files = Vec::new();
    for table in &lake.tables {
        let info = table.info.as_ref().ok_or_else(|| {
            PyValueError::new_err(format!(
                "table {} has no log; pass it to `table` first",
                table.name
            ))
        })?;
        files.extend(files_from_table(info, Some(table.name.as_str())));
    }
    Ok(files)
}

fn viz_rows(obj: &Bound<'_, PyAny>) -> PyResult<Vec<MassRecord>> {
    let value = py_to_value(obj)?;
    let items = match value {
        Value::Array(items) => items,
        Value::Object(fields)
            if fields.get("kind").and_then(Value::as_str) == Some("pqbench.bytemass-row") =>
        {
            vec![Value::Object(fields)]
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "viz reads a bytemass row stream, not `{}`",
                other
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or("document")
            )));
        }
    };
    items
        .into_iter()
        .map(|item| {
            let id = item
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            Ok(MassRecord {
                id,
                file: string_field(&item, "file")?,
                size: u64_field(&item, "size")?,
                num_rows: u64_field(&item, "num_rows")?,
                column: string_field(&item, "column")?,
                compressed_bytes: u64_field(&item, "compressed_bytes")?,
                uncompressed_bytes: u64_field(&item, "uncompressed_bytes")?,
                codec: string_field(&item, "codec")?,
            })
        })
        .collect()
}

fn string_field(value: &Value, name: &str) -> PyResult<String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| PyValueError::new_err(format!("bytemass row needs `{name}`")))
}

fn u64_field(value: &Value, name: &str) -> PyResult<u64> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| PyValueError::new_err(format!("bytemass row needs `{name}`")))
}

fn block_on<T, E>(future: impl std::future::Future<Output = Result<T, E>>) -> Result<T, String>
where
    E: ToString,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?;
    runtime.block_on(future).map_err(|err| err.to_string())
}

fn runtime(err: impl ToString) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn dumps(py: Python<'_>, value: impl Serialize) -> PyResult<Py<PyAny>> {
    let value = serde_json::to_value(value).map_err(runtime)?;
    value_to_py(py, &value)
}

fn to_object(py: Python<'_>, body: &str) -> PyResult<Py<PyAny>> {
    let value: Value = serde_json::from_str(body).map_err(runtime)?;
    value_to_py(py, &value)
}

fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(flag) => {
            let bound = (*flag).into_pyobject(py)?.to_owned();
            Ok(bound.into_any().unbind())
        }
        Value::Number(number) => {
            if let Some(value) = number.as_u64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else if let Some(value) = number.as_i64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else if let Some(value) = number.as_f64() {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            } else {
                Err(PyValueError::new_err("unsupported JSON number"))
            }
        }
        Value::String(text) => Ok(text.into_pyobject(py)?.into_any().unbind()),
        Value::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        Value::Object(fields) => {
            let dict = PyDict::new(py);
            for (key, item) in fields {
                dict.set_item(key, value_to_py(py, item)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(flag) = obj.extract::<bool>() {
        return Ok(Value::Bool(flag));
    }
    if let Ok(value) = obj.extract::<u64>() {
        return Ok(Value::from(value));
    }
    if let Ok(value) = obj.extract::<i64>() {
        return Ok(Value::from(value));
    }
    if let Ok(value) = obj.extract::<f64>() {
        return Ok(serde_json::Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null));
    }
    if let Ok(text) = obj.extract::<String>() {
        return Ok(Value::String(text));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        let mut items = Vec::with_capacity(list.len());
        for item in list.iter() {
            items.push(py_to_value(&item)?);
        }
        return Ok(Value::Array(items));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut fields = serde_json::Map::new();
        for (key, item) in dict.iter() {
            fields.insert(key.extract::<String>()?, py_to_value(&item)?);
        }
        return Ok(Value::Object(fields));
    }
    Err(PyTypeError::new_err(format!(
        "cannot convert {} to a document",
        obj.get_type().name()?
    )))
}

fn as_text(py: Python<'_>, body: String) -> PyResult<Py<PyAny>> {
    Ok(body.into_pyobject(py)?.into_any().unbind())
}
