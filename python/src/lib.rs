//! In-process bindings for the pqbench commands.
//!
//! Each function calls the same library entry point as the matching CLI
//! subcommand and returns the same document, bytes, or paths the CLI would
//! produce.
//!
//! The **walk** is the verb-first surface: `get_*` reads one resource and
//! `list_*` enumerates a collection, mirroring `pqbench get …` and `pqbench
//! list …`. The resources are the `pqbench.v1` wire types, so a value returned
//! here round-trips as JSON into `bytemass`, `list_files`, or any CLI stage.
//! The analysis commands (`lz`, `compression`, `bytemass`, `diff`, `profile`,
//! `experiment`, `viz`, `skill`) read storage or a sample directly, and
//! `ratelimit` and `setup` mirror their CLI helpers.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use futures::StreamExt;
use pqbench::experiment::{Aim, ExperimentRequest};
use pqbench::profile::ProfileRequest;
use pqbench::stats;
use pqbench::third_party::parquet::api::{read_sample, read_typed_sample};
use pqbench::v1::{Credentials, Partition, TableFile};
use pqbench::viz::MassRecord;
use pqbench::{catalog, file, metastore, partition, schema, v1, Config, Dialect, Lake, Workspace};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Py, PyAny};
use serde::Serialize;
use serde_json::Value;

/// The default `ratelimit` rate, matching `pqbench::ratelimit::DEFAULT_REQUESTS_PER_SECOND`.
const DEFAULT_RATE: f64 = 15.0;

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
#[pyo3(signature = (*inputs, env=None, indexes=false))]
fn bytemass(
    py: Python<'_>,
    inputs: Vec<String>,
    env: Option<BTreeMap<String, String>>,
    indexes: bool,
) -> PyResult<Py<PyAny>> {
    let request = pqbench::bytemass::BytemassRequest {
        inputs,
        env: aws_env(env)?,
        indexes,
    };
    let (_workspace, lake) = capabilities(None, None, None, false)?;
    let rows = py
        .detach(|| block_on(pqbench::bytemass::bytemass(&lake, &request)))
        .map_err(runtime)?;
    dumps(py, &rows)
}

/// Run `pqbench profile`: decode a bounded row sample into per-column facts.
///
/// `columns` is a list of column-name globs (default: all). `rows` is `all` or
/// `first:N`. Returns the stream of `pqbench.profile-column` objects (one dict
/// per column, carrying the input path as `id`).
#[pyfunction]
#[pyo3(signature = (*inputs, columns=None, rows="first:8192", top=8))]
fn profile(
    py: Python<'_>,
    inputs: Vec<String>,
    columns: Option<Vec<String>>,
    rows: &str,
    top: u32,
) -> PyResult<Py<PyAny>> {
    let max_rows = parse_profile_rows(rows)?;
    let request = ProfileRequest {
        columns: columns.unwrap_or_default(),
        top,
    };
    let mut records: Vec<Value> = Vec::new();
    for input in &inputs {
        let sample = py
            .detach(|| read_sample(Path::new(input), max_rows))
            .map_err(runtime)?;
        let report = pqbench::profile::profile(&sample, &request).map_err(runtime)?;
        for column in report.columns {
            let mut object = serde_json::Map::new();
            object.insert("kind".into(), Value::from("pqbench.profile-column"));
            object.insert("id".into(), Value::from(input.clone()));
            if let Value::Object(fields) = serde_json::to_value(&column).map_err(runtime)? {
                object.extend(fields);
            }
            records.push(Value::Object(object));
        }
    }
    dumps(py, &records)
}

/// Run `pqbench experiment`: rewrite a decoded sample and measure it.
///
/// `rewrite` and `trial` are repeatable specs; `aim` is `storage`, `skipping`,
/// or `all`. `rows` is `all` or `first:N`. Returns the stream of
/// `pqbench.experiment-trial` objects (the control first).
#[pyfunction]
#[pyo3(signature = (path, *, rewrite=None, trial=None, aim="storage", rows="first:8192"))]
fn experiment(
    py: Python<'_>,
    path: PathBuf,
    rewrite: Option<Vec<String>>,
    trial: Option<Vec<String>>,
    aim: &str,
    rows: &str,
) -> PyResult<Py<PyAny>> {
    let max_rows = parse_profile_rows(rows)?;
    let mut trials = rewrite.unwrap_or_default();
    trials.extend(trial.unwrap_or_default());
    let request = ExperimentRequest {
        trials,
        aim: Aim::parse(aim).map_err(runtime)?,
    };
    let sample = py
        .detach(|| read_typed_sample(&path, max_rows))
        .map_err(runtime)?;
    let report = pqbench::experiment::experiment(&sample, &request).map_err(runtime)?;
    let mut records: Vec<Value> = Vec::new();
    for trial in report.trials {
        let mut object = serde_json::Map::new();
        object.insert("kind".into(), Value::from("pqbench.experiment-trial"));
        if let Value::Object(fields) = serde_json::to_value(&trial).map_err(runtime)? {
            object.extend(fields);
        }
        records.push(Value::Object(object));
    }
    dumps(py, &records)
}

/// Run `pqbench diff`: compare two bytemass measurements.
///
/// `left` and `right` are each a parquet path or glob mask (measured here) or a
/// list of `pqbench.bytemass-row` dicts. `depth` rolls dotted physical column
/// paths up to that many components. Returns the stream of `pqbench.diff-column`
/// objects.
#[pyfunction]
#[pyo3(signature = (left, right, *, depth=None, endpoint=None, token=None, table_format=None))]
fn diff(
    py: Python<'_>,
    left: Bound<'_, PyAny>,
    right: Bound<'_, PyAny>,
    depth: Option<usize>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (_workspace, lake) = capabilities(endpoint, token, table_format, false)?;
    let left = side_rows(py, &left, &lake)?;
    let right = side_rows(py, &right, &lake)?;
    let deltas = pqbench::diff::compare(&left, &right, depth).map_err(runtime)?;
    let records: Vec<Value> = deltas
        .into_iter()
        .map(|delta| with_kind("pqbench.diff-column", &delta))
        .collect::<PyResult<_>>()?;
    dumps(py, &records)
}

/// Run `pqbench ratelimit`: pace a list of records to `rate` per second.
///
/// Records pass through unchanged, delayed so each resource type observes at
/// most `rate` records per second; `0` turns pacing off. The bucket is a
/// record's `kind`, or the collection segment of its `name`.
#[pyfunction]
#[pyo3(signature = (records, *, rate=DEFAULT_RATE))]
fn ratelimit(py: Python<'_>, records: Bound<'_, PyAny>, rate: f64) -> PyResult<Py<PyAny>> {
    if !rate.is_finite() || rate < 0.0 {
        return Err(PyValueError::new_err(format!(
            "rate expects a non-negative rate, got {rate}"
        )));
    }
    let items = value_items(&py_to_value(&records)?);
    let mut limit = pqbench::ratelimit::RateLimit::new(rate);
    for record in &items {
        let delay = limit.delay(record_type(record), Instant::now());
        if !delay.is_zero() {
            py.detach(|| std::thread::sleep(delay));
        }
    }
    dumps(py, &items)
}

/// Run `pqbench setup`: the environment the resolved configuration exports.
///
/// Returns a dict of `PQB_*` names (plus `AWS_REGION` when `region` is set and
/// `AWS_EC2_METADATA_DISABLED`), the Python spelling of the shell `export`
/// lines. Needs an endpoint and a token, like the CLI.
#[pyfunction]
#[pyo3(signature = (*, region=None, endpoint=None, token=None, table_format=None))]
fn setup(
    py: Python<'_>,
    region: Option<&str>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let config = resolve_config(endpoint, token, table_format, true)?;
    let token = config.token.as_deref().ok_or_else(|| {
        PyValueError::new_err(
            "setup needs a token: pass token= or set PQB_TOKEN / DATABRICKS_TOKEN",
        )
    })?;
    let mut out = serde_json::Map::new();
    out.insert("PQB_ENDPOINT".into(), Value::String(config.endpoint));
    out.insert("PQB_TOKEN".into(), Value::String(token.to_string()));
    out.insert(
        "PQB_TABLE_FORMAT".into(),
        Value::String(dialect_name(config.dialect).to_string()),
    );
    if let Some(region) = region.map(str::trim).filter(|value| !value.is_empty()) {
        out.insert("AWS_REGION".into(), Value::String(region.to_string()));
    }
    out.insert(
        "AWS_EC2_METADATA_DISABLED".into(),
        Value::String("true".into()),
    );
    value_to_py(py, &Value::Object(out))
}

/// Run `pqbench skill`.
///
/// No `name` lists skills. `document` is `recipes` for the extra markdown.
#[pyfunction]
#[pyo3(signature = (name=None, document=""))]
fn skill(py: Python<'_>, name: Option<&str>, document: &str) -> PyResult<Py<PyAny>> {
    match name {
        None => {
            let items: Vec<Value> = pqbench::skill::skills()
                .iter()
                .map(|item| {
                    let mut object = serde_json::Map::new();
                    object.insert("name".into(), Value::String(item.name.into()));
                    object.insert("description".into(), Value::String(item.description.into()));
                    Value::Object(object)
                })
                .collect();
            dumps(py, &items)
        }
        Some(name) => {
            let markdown = pqbench::skill::document(name, document).map_err(runtime)?;
            as_text(py, markdown.to_string())
        }
    }
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
    py.detach(|| pqbench::viz::write_report(&prefix, &records, &[]))
        .map_err(runtime)?;
    let mut result = serde_json::Map::new();
    result.insert(
        "html".into(),
        Value::String(prefix.with_extension("html").display().to_string()),
    );
    value_to_py(py, &Value::Object(result))
}

/// `pqbench get metastore`: the endpoint's metastore record.
#[pyfunction]
#[pyo3(signature = (*, endpoint=None, token=None, table_format=None))]
fn get_metastore(
    py: Python<'_>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (workspace, _lake) = capabilities(endpoint, token, table_format, true)?;
    let value = py
        .detach(|| block_on(metastore::get::read(&workspace)))
        .map_err(runtime)?;
    dumps(py, &value)
}

/// `pqbench get catalog CATALOG`: one catalog's record.
#[pyfunction]
#[pyo3(signature = (catalog, *, endpoint=None, token=None, table_format=None))]
fn get_catalog(
    py: Python<'_>,
    catalog: String,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (workspace, _lake) = capabilities(endpoint, token, table_format, true)?;
    let value = py
        .detach(|| block_on(catalog::get::read(&workspace, &catalog)))
        .map_err(runtime)?;
    dumps(py, &value)
}

/// `pqbench get schema CATALOG.SCHEMA`: one schema's record.
#[pyfunction]
#[pyo3(signature = (catalog, schema, *, endpoint=None, token=None, table_format=None))]
fn get_schema(
    py: Python<'_>,
    catalog: String,
    schema: String,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (workspace, _lake) = capabilities(endpoint, token, table_format, true)?;
    let value = py
        .detach(|| block_on(schema::get::read(&workspace, &catalog, &schema)))
        .map_err(runtime)?;
    dumps(py, &value)
}

/// `pqbench get table CATALOG.SCHEMA.TABLE`: one table's record.
///
/// `location` is a storage path a parent ref already carries, when it does.
/// `env` carries the object-store options (`AWS_*` names) the metadata read
/// needs.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (catalog, schema, table, *, location=None, env=None, endpoint=None, token=None, table_format=None))]
fn get_table(
    py: Python<'_>,
    catalog: String,
    schema: String,
    table: String,
    location: Option<String>,
    env: Option<BTreeMap<String, String>>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (workspace, lake) = capabilities(endpoint, token, table_format, true)?;
    let storage = pqbench::table::get::Storage {
        location,
        env: aws_env(env)?,
    };
    let value = py
        .detach(|| {
            block_on(pqbench::table::get::read(
                &workspace, &lake, &catalog, &schema, &table, &storage,
            ))
        })
        .map_err(runtime)?;
    dumps(py, &value)
}

/// `pqbench list catalogs`: the catalogs at the endpoint.
#[pyfunction]
#[pyo3(signature = (*, endpoint=None, token=None, table_format=None))]
fn list_catalogs(
    py: Python<'_>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (workspace, _lake) = capabilities(endpoint, token, table_format, true)?;
    let value = py
        .detach(|| block_on(catalog::ls::list(&workspace)))
        .map_err(runtime)?;
    dumps(py, &value)
}

/// `pqbench list schemas CATALOG`: the schemas in a catalog.
#[pyfunction]
#[pyo3(signature = (catalog, *, endpoint=None, token=None, table_format=None))]
fn list_schemas(
    py: Python<'_>,
    catalog: String,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (workspace, _lake) = capabilities(endpoint, token, table_format, true)?;
    let value = py
        .detach(|| block_on(schema::ls::list(&workspace, &catalog)))
        .map_err(runtime)?;
    dumps(py, &value)
}

/// `pqbench list tables CATALOG.SCHEMA`: the tables in a schema.
#[pyfunction]
#[pyo3(signature = (catalog, schema, *, endpoint=None, token=None, table_format=None))]
fn list_tables(
    py: Python<'_>,
    catalog: String,
    schema: String,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (workspace, _lake) = capabilities(endpoint, token, table_format, true)?;
    let value = py
        .detach(|| block_on(pqbench::table::ls::list(&workspace, &catalog, &schema)))
        .map_err(runtime)?;
    dumps(py, &value)
}

/// `pqbench list partitions URI`: a table's natural partitions.
///
/// `table` overrides the resource name (default: the `local` name a storage
/// root takes). `every` is the window width, epoch-aligned: `1h`, `1d`, `1w`.
/// `env` carries the object-store options the log read needs. Returns the
/// stream of `Partition` objects as a list.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (uri, *, table=None, every="1d", env=None, endpoint=None, token=None, table_format=None))]
fn list_partitions(
    py: Python<'_>,
    uri: String,
    table: Option<String>,
    every: &str,
    env: Option<BTreeMap<String, String>>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let window = parse_window(every)?;
    let table = table.unwrap_or_else(|| v1::name::local_table(&uri));
    let env = aws_env(env)?;
    let (_workspace, lake) = capabilities(endpoint, token, table_format, false)?;
    let partitions = py
        .detach(|| {
            block_on(async {
                let mut stream = partition::ls::list(&lake, &table, &uri, &env, window)
                    .await
                    .map_err(|error| error.to_string())?;
                let mut items = Vec::new();
                while let Some(item) = stream.next().await {
                    items.push(item.map_err(|error| error.to_string())?);
                }
                Ok::<Vec<Partition>, String>(items)
            })
        })
        .map_err(runtime)?;
    dumps(py, &partitions)
}

/// `pqbench list files`: the files a partition's commits added.
///
/// `partition` is a `Partition` (as returned by [`list_partitions`]); its
/// `uri`, `commit_versions`, and `credentials` name the log to read. `env`
/// carries extra object-store options; the partition's own lease wins.
#[pyfunction]
#[pyo3(signature = (partition, *, env=None, endpoint=None, token=None, table_format=None))]
fn list_files(
    py: Python<'_>,
    partition: Bound<'_, PyAny>,
    env: Option<BTreeMap<String, String>>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let partition: Partition = serde_json::from_value(py_to_value(&partition)?)
        .map_err(|error| PyValueError::new_err(format!("not a partition record: {error}")))?;
    let (catalog, schema, table, _partition) = v1::name::parse_partition(&partition.name)
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "a partition name must be `…/tables/{{table}}/partitions/{{partition}}`: {}",
                partition.name
            ))
        })?;
    let uri = partition.uri.clone().ok_or_else(|| {
        PyValueError::new_err("a partition record needs a table storage root (uri)")
    })?;
    let mut env = aws_env(env)?;
    if let Some(credentials) = &partition.credentials {
        env.extend(credentials.to_env());
    }
    let versions = partition.commit_versions.clone();
    let (_workspace, lake) = capabilities(endpoint, token, table_format, false)?;
    let found = py
        .detach(|| {
            block_on(async {
                file::ls::list(&lake, &uri, &env, &versions)
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .map_err(runtime)?;
    let records: Vec<TableFile> = found
        .into_iter()
        .map(|file| TableFile {
            name: v1::name::table_file(&catalog, &schema, &table, &file.path),
            blob: Some(v1::name::blob_name(&file.uri)),
            size_bytes: Some(file.size_bytes),
            credentials: partition.credentials.clone(),
            ..TableFile::default()
        })
        .collect();
    dumps(py, &records)
}

/// `pqbench credentials get`: put vended read credentials on each table-ref.
///
/// `refs` is one table-ref dict or a list of them (a `Table` from
/// [`list_tables`]). Each ref the catalog vends for gains its `credentials`;
/// a table the catalog vends nothing for passes through unchanged.
#[pyfunction]
#[pyo3(signature = (refs, *, endpoint=None, token=None, table_format=None))]
fn credentials_get(
    py: Python<'_>,
    refs: Bound<'_, PyAny>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let (workspace, _lake) = capabilities(endpoint, token, table_format, true)?;
    let records = refs_value(&refs)?;
    let mut out = Vec::with_capacity(records.len());
    for record in records {
        let (catalog, schema, table) = table_name(&record)?;
        let vended = py
            .detach(|| block_on(vend(&workspace, &catalog, &schema, &table)))
            .map_err(runtime)?;
        out.push(attach_credentials(record, vended));
    }
    dumps(py, &out)
}

/// `pqbench credentials check`: the table-refs readable outside Databricks.
///
/// `refs` is one table-ref dict or a list of them. Returns the refs the
/// catalog reports readable (a `system` table, a view, and managed default
/// storage are dropped), so the result composes ahead of [`credentials_get`].
#[pyfunction]
#[pyo3(signature = (refs, *, endpoint=None, token=None, table_format=None))]
fn credentials_check(
    py: Python<'_>,
    refs: Bound<'_, PyAny>,
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
) -> PyResult<Py<PyAny>> {
    use pqbench::credentials::check::Eligibility;
    let (workspace, _lake) = capabilities(endpoint, token, table_format, true)?;
    let records = refs_value(&refs)?;
    let mut out = Vec::with_capacity(records.len());
    for record in records {
        let (catalog, schema, table) = table_name(&record)?;
        let eligibility = py
            .detach(|| {
                block_on(pqbench::credentials::check::check(
                    &workspace, &catalog, &schema, &table,
                ))
            })
            .map_err(runtime)?;
        if matches!(eligibility, Eligibility::Eligible) {
            out.push(record);
        }
    }
    dumps(py, &out)
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // pqbench reads tables from outside Databricks with explicit credentials
    // (vended, or on the source); it never relies on EC2 instance metadata.
    // The AWS SDK probes IMDS for the region otherwise and hangs where that
    // endpoint is blackholed; the CLI defaults the probe off at startup, and
    // the module matches it. A caller that sets it wins.
    if std::env::var_os("AWS_EC2_METADATA_DISABLED").is_none() {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
    module.add_function(wrap_pyfunction!(lz, module)?)?;
    module.add_function(wrap_pyfunction!(compression, module)?)?;
    module.add_function(wrap_pyfunction!(bytemass, module)?)?;
    module.add_function(wrap_pyfunction!(profile, module)?)?;
    module.add_function(wrap_pyfunction!(experiment, module)?)?;
    module.add_function(wrap_pyfunction!(diff, module)?)?;
    module.add_function(wrap_pyfunction!(ratelimit, module)?)?;
    module.add_function(wrap_pyfunction!(setup, module)?)?;
    module.add_function(wrap_pyfunction!(skill, module)?)?;
    module.add_function(wrap_pyfunction!(viz, module)?)?;
    module.add_function(wrap_pyfunction!(get_metastore, module)?)?;
    module.add_function(wrap_pyfunction!(get_catalog, module)?)?;
    module.add_function(wrap_pyfunction!(get_schema, module)?)?;
    module.add_function(wrap_pyfunction!(get_table, module)?)?;
    module.add_function(wrap_pyfunction!(list_catalogs, module)?)?;
    module.add_function(wrap_pyfunction!(list_schemas, module)?)?;
    module.add_function(wrap_pyfunction!(list_tables, module)?)?;
    module.add_function(wrap_pyfunction!(list_partitions, module)?)?;
    module.add_function(wrap_pyfunction!(list_files, module)?)?;
    module.add_function(wrap_pyfunction!(credentials_get, module)?)?;
    module.add_function(wrap_pyfunction!(credentials_check, module)?)?;
    module.add("commands", PyTuple::new(module.py(), COMMANDS)?)?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

/// Every bound command, in the order the CLI's help groups them.
const COMMANDS: [&str; 21] = [
    "lz",
    "compression",
    "bytemass",
    "profile",
    "experiment",
    "diff",
    "ratelimit",
    "setup",
    "skill",
    "viz",
    "get_metastore",
    "get_catalog",
    "get_schema",
    "get_table",
    "list_catalogs",
    "list_schemas",
    "list_tables",
    "list_partitions",
    "list_files",
    "credentials_get",
    "credentials_check",
];

fn parse_mode(mode: &str) -> PyResult<stats::Mode> {
    match mode {
        "fastest" => Ok(stats::Mode::Fastest),
        "mean" => Ok(stats::Mode::Mean),
        other => Err(PyValueError::new_err(format!(
            "invalid mode {other:?}; expected fastest or mean"
        ))),
    }
}

fn parse_dialect(value: &str) -> PyResult<Dialect> {
    match value {
        "unity" => Ok(Dialect::Unity),
        "iceberg" => Ok(Dialect::Iceberg),
        other => Err(PyValueError::new_err(format!(
            "invalid table_format {other:?}; expected unity or iceberg"
        ))),
    }
}

fn dialect_name(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Unity => "unity",
        Dialect::Iceberg => "iceberg",
    }
}

/// Parse `rows`: `all` reads every row, `first:N` reads N (N >= 1).
fn parse_profile_rows(method: &str) -> PyResult<Option<usize>> {
    if method == "all" {
        return Ok(None);
    }
    if let Some(rest) = method.strip_prefix("first:") {
        let rows: usize = rest.parse().map_err(|_| {
            PyValueError::new_err(format!("bad rows {method:?}; expected `all` or `first:N`"))
        })?;
        if rows == 0 {
            return Err(PyValueError::new_err(
                "bad rows `first:0`; N must be at least 1",
            ));
        }
        return Ok(Some(rows));
    }
    Err(PyValueError::new_err(format!(
        "bad rows {method:?}; expected `all` or `first:N`"
    )))
}

/// Parse a window width: a positive integer and a unit (`m`, `h`, `d`, `w`).
fn parse_window(value: &str) -> PyResult<i64> {
    let (number, unit) = value.split_at(
        value
            .len()
            .checked_sub(1)
            .ok_or_else(|| PyValueError::new_err("every is empty"))?,
    );
    let count: i64 = number
        .parse()
        .map_err(|_| PyValueError::new_err(format!("every `{value}` needs a number and a unit")))?;
    if count <= 0 {
        return Err(PyValueError::new_err(format!(
            "every must be positive: {value}"
        )));
    }
    let millis = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => {
            return Err(PyValueError::new_err(format!(
                "every unit must be m, h, d, or w: {value}"
            )))
        }
    };
    Ok(count * millis)
}

/// The resolved configuration: explicit keyword arguments win over `PQB_*`,
/// which wins over `DATABRICKS_*`, as the CLI's global flags do.
fn resolve_config(
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
    require_endpoint: bool,
) -> PyResult<Config> {
    let endpoint = endpoint
        .map(str::to_owned)
        .or_else(|| env_non_empty("PQB_ENDPOINT"))
        .or_else(|| env_non_empty("DATABRICKS_HOST"))
        .unwrap_or_default();
    let token = token
        .map(str::to_owned)
        .or_else(|| env_non_empty("PQB_TOKEN"))
        .or_else(|| env_non_empty("DATABRICKS_TOKEN"));
    let dialect = match table_format
        .map(str::to_owned)
        .or_else(|| env_non_empty("PQB_TABLE_FORMAT"))
    {
        Some(value) => parse_dialect(&value)?,
        None => Dialect::Unity,
    };
    if require_endpoint && endpoint.is_empty() {
        return Err(PyValueError::new_err(
            "an endpoint is required: pass endpoint= or set PQB_ENDPOINT / DATABRICKS_HOST",
        ));
    }
    Ok(Config {
        endpoint,
        token,
        dialect,
    })
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// The workspace and lake a command runs under: the resolved configuration
/// and the storage registry built over the same client.
fn capabilities(
    endpoint: Option<&str>,
    token: Option<&str>,
    table_format: Option<&str>,
    require_endpoint: bool,
) -> PyResult<(Workspace, Lake)> {
    let config = resolve_config(endpoint, token, table_format, require_endpoint)?;
    let workspace = Workspace::new(config).map_err(runtime)?;
    let lake = Lake::new(&workspace);
    Ok((workspace, lake))
}

/// Vendor read credentials for one table, by the workspace's dialect.
async fn vend(
    workspace: &Workspace,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Option<BTreeMap<String, String>>, String> {
    let vended = match workspace.config().dialect {
        Dialect::Unity => {
            pqbench::credentials::get::vend_unity(workspace, catalog, schema, table).await
        }
        Dialect::Iceberg => pqbench::credentials::get::vend_iceberg(workspace, schema, table).await,
    };
    vended.map_err(|error| error.to_string())
}

/// The ref with the vended lease written onto it as `credentials`; an older
/// `env` bag is dropped, so a table's lease lives in one place. A ref the
/// catalog vends nothing for passes through unchanged.
fn attach_credentials(mut record: Value, env: Option<BTreeMap<String, String>>) -> Value {
    let Some(env) = env else {
        return record;
    };
    let credentials = Credentials::from_env(&env);
    if credentials.is_empty() {
        return record;
    }
    let Some(object) = record.as_object_mut() else {
        return record;
    };
    object.remove("env");
    object.insert(
        "credentials".into(),
        serde_json::to_value(credentials).unwrap_or(Value::Null),
    );
    record
}

/// The catalog, schema, and table a ref's `name` parses to.
fn table_name(record: &Value) -> PyResult<(String, String, String)> {
    let name = record
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| PyValueError::new_err("a table-ref needs a name"))?;
    v1::name::parse_table(name).ok_or_else(|| {
        PyValueError::new_err(format!(
            "a table-ref name must be `…/tables/{{table}}`: {name}"
        ))
    })
}

/// One or more records as a list: a single object is one record.
fn refs_value(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
    match py_to_value(obj)? {
        Value::Array(items) => Ok(items),
        object @ Value::Object(_) => Ok(vec![object]),
        other => Err(PyValueError::new_err(format!(
            "expected a record or a list of records, not `{}`",
            json_kind(&other)
        ))),
    }
}

/// One diff side as bytemass rows: a path or glob mask is measured, a list of
/// rows is taken as-is.
fn side_rows(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    lake: &Lake,
) -> PyResult<Vec<pqbench::bytemass::MassRow>> {
    if let Ok(input) = obj.extract::<String>() {
        let request = pqbench::bytemass::BytemassRequest {
            inputs: vec![input],
            ..Default::default()
        };
        return py
            .detach(|| block_on(pqbench::bytemass::bytemass(lake, &request)))
            .map_err(runtime);
    }
    value_items(&py_to_value(obj)?)
        .into_iter()
        .map(|item| {
            serde_json::from_value(item).map_err(|error| {
                PyValueError::new_err(format!("diff reads bytemass rows: {error}"))
            })
        })
        .collect()
}

/// The items of a JSON value: an array stays, anything else is one item.
fn value_items(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items.clone(),
        other => vec![other.clone()],
    }
}

/// A value tagged with a `kind`, as the CLI's row records are.
fn with_kind<T: Serialize>(kind: &'static str, value: &T) -> PyResult<Value> {
    let mut object = serde_json::Map::new();
    object.insert("kind".into(), Value::from(kind));
    if let Value::Object(fields) =
        serde_json::to_value(value).map_err(|error| PyRuntimeError::new_err(error.to_string()))?
    {
        object.extend(fields);
    }
    Ok(Value::Object(object))
}

/// The resource type a record names — the pacing bucket: its `kind`, or the
/// collection segment of its `name` (the segment before the id).
fn record_type(record: &Value) -> &str {
    if let Some(kind) = record.get("kind").and_then(Value::as_str) {
        return kind;
    }
    let Some(name) = record.get("name").and_then(Value::as_str) else {
        return "";
    };
    let segments: Vec<&str> = name.split('/').collect();
    segments
        .len()
        .checked_sub(2)
        .and_then(|index| segments.get(index))
        .copied()
        .unwrap_or("")
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
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
                encodings: item
                    .get("encodings")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join(",")
                    })
                    .or_else(|| {
                        item.get("encodings")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
                    .unwrap_or_default(),
                num_values: item.get("num_values").and_then(Value::as_u64).unwrap_or(0),
                dictionary: item
                    .get("dictionary")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                null_count: item.get("null_count").and_then(Value::as_u64),
                distinct_count: item.get("distinct_count").and_then(Value::as_u64),
                physical_type: item
                    .get("physical_type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                row_group: item.get("row_group").and_then(Value::as_u64).unwrap_or(0) as u32,
                row_group_rows: item
                    .get("row_group_rows")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                compressed_bytes_per_row: item
                    .get("compressed_bytes_per_row")
                    .and_then(Value::as_f64),
                page_count: item.get("page_count").and_then(Value::as_u64),
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
