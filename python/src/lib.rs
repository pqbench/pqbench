//! In-process bindings for the pqbench commands.
//!
//! Each function calls the same library entry point as the matching CLI
//! subcommand and returns the same document, bytes, or paths the CLI would
//! produce.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use pqbench::dump::DumpFile;
use pqbench::lake::Lake;
use pqbench::stats;
use pqbench::table::{LoadRequest, TableInfo};
use pqbench::viz::MassRecord;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
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
#[pyo3(signature = (uri, *, version=None, env=None))]
fn table(
    py: Python<'_>,
    uri: String,
    version: Option<u64>,
    env: Option<BTreeMap<String, String>>,
) -> PyResult<Py<PyAny>> {
    let request = pqbench::table::LoadRequest::new(uri, version, aws_env(env)?);
    let info = py
        .detach(|| block_on(pqbench::table::load(&request)))
        .map_err(runtime)?;
    dumps(py, &info)
}

/// Run `pqbench lake`: list tables under a directory or URI.
///
/// Returns the `pqbench.lake` document.
#[pyfunction]
#[pyo3(signature = (root, *, max_depth=None))]
fn lake(py: Python<'_>, root: String, max_depth: Option<usize>) -> PyResult<Py<PyAny>> {
    let env = BTreeMap::new();
    let lake = py
        .detach(move || {
            block_on(pqbench::lake::discover_bounded(
                &root,
                &env,
                max_depth,
                |_| true,
            ))
        })
        .map_err(runtime)?;
    dumps(py, &lake)
}

/// Run `pqbench dump`: copy the Parquet files a table or lake names into
/// `output`.
///
/// `inputs` are table URIs or a `pqbench.table` / `pqbench.lake` document.
/// Returns `{"file_count": N, "byte_count": N}`.
#[pyfunction]
#[pyo3(signature = (output, *inputs))]
fn dump(py: Python<'_>, output: PathBuf, inputs: Vec<Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
    let (tables, lakes, uris) = dump_inputs(&inputs)?;
    let mut entries = dump_document_entries(&tables, &lakes)?;
    let summary = py
        .detach(|| {
            block_on(async {
                for uri in &uris {
                    let info =
                        pqbench::table::load(&LoadRequest::new(uri.clone(), None, BTreeMap::new()))
                            .await
                            .map_err(|error| error.to_string())?;
                    push_table(&mut entries, &info, info.uri.clone());
                }
                pqbench::dump::put(&nest_entries(entries), &output)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .map_err(runtime)?;
    let mut result = serde_json::Map::new();
    result.insert("file_count".into(), Value::from(summary.file_count));
    result.insert("byte_count".into(), Value::from(summary.byte_count));
    value_to_py(py, &Value::Object(result))
}

/// Run `pqbench viz` on a bytemass row stream.
///
/// Writes `output.html`. Returns that path.
#[pyfunction]
#[pyo3(signature = (rows, *, output))]
fn viz(py: Python<'_>, rows: Bound<'_, PyAny>, output: PathBuf) -> PyResult<Py<PyAny>> {
    let records = viz_rows(&rows)?;
    let prefix = match output.extension().and_then(|ext| ext.to_str()) {
        Some("html" | "htm") => output.with_extension(""),
        _ => output,
    };
    py.detach(|| pqbench::viz::write_report(&prefix, &records))
        .map_err(runtime)?;
    let mut result = serde_json::Map::new();
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

/// Split `dump` inputs into serde documents and table URIs.
fn dump_inputs(inputs: &[Bound<'_, PyAny>]) -> PyResult<(Vec<TableInfo>, Vec<Lake>, Vec<String>)> {
    let mut tables = Vec::new();
    let mut lakes = Vec::new();
    let mut uris = Vec::new();
    for input in inputs {
        let Ok(value) = py_to_value(input) else {
            uris.push(input.extract::<String>()?);
            continue;
        };
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .map(str::to_string);
        match kind.as_deref() {
            Some("pqbench.table") => tables.push(serde_json::from_value(value).map_err(runtime)?),
            Some("pqbench.lake") => lakes.push(serde_json::from_value(value).map_err(runtime)?),
            _ => uris.push(input.extract::<String>()?),
        }
    }
    Ok((tables, lakes, uris))
}

/// A file's table id plus the file, in document order.
type Entry = (String, DumpFile);

fn dump_document_entries(tables: &[TableInfo], lakes: &[Lake]) -> PyResult<Vec<Entry>> {
    let mut entries = Vec::new();
    for info in tables {
        push_table(&mut entries, info, info.uri.clone());
    }
    for lake in lakes {
        for table in &lake.tables {
            let info = table.info.as_ref().ok_or_else(|| {
                PyValueError::new_err(format!(
                    "table {} has no log; pass it to `table` first",
                    table.name
                ))
            })?;
            push_table(&mut entries, info, table.name.clone());
        }
    }
    Ok(entries)
}

fn push_table(entries: &mut Vec<Entry>, info: &TableInfo, id: String) {
    for file in &info.files {
        entries.push((
            id.clone(),
            DumpFile {
                path: file.path.clone(),
                uri: file.uri.clone(),
                env: info.env.clone(),
            },
        ));
    }
}

/// Nest files under their table id when more than one table is dumped, so files
/// with the same relative path do not collide.
fn nest_entries(entries: Vec<Entry>) -> Vec<DumpFile> {
    let multiple = entries
        .iter()
        .map(|(id, _)| id)
        .collect::<BTreeSet<_>>()
        .len()
        > 1;
    entries
        .into_iter()
        .map(|(id, mut file)| {
            if multiple {
                file.path = format!("{id}/{}", file.path);
            }
            file
        })
        .collect()
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
                file: string_field(&item, "uri")?,
                size: u64_field(&item, "size_bytes")?,
                row_count: u64_field(&item, "row_count")?,
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
