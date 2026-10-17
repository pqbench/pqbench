//! The `pqbench` library, exposed to Python as `pqbench.native`.
//!
//! Every name is the library function of the same path, with the same
//! arguments: `pqbench.native.metastore.get.read(workspace)`,
//! `pqbench.native.catalog.ls.list(workspace)`,
//! `pqbench.native.file.ls.list(lake, uri, env, versions)`,
//! `pqbench.native.credentials.check.check(workspace, catalog, schema, table)`.
//!
//! The client handles — `Config`, `Layer`, `Workspace`, `Lake` — and the
//! request structs (`Storage`, `LzRequest`, `CompressionRequest`,
//! `BytemassRequest`, `ProfileRequest`, `ExperimentRequest`) are Python
//! classes. The records the library returns are its `Serialize` types,
//! marshalled as dicts: a collection the library streams is an async iterator,
//! a single value an awaitable.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use pqbench::bytemass::BytemassRequest as LibBytemassRequest;
use pqbench::compression::CompressionRequest as LibCompressionRequest;
use pqbench::experiment::{Aim as LibAim, ExperimentRequest as LibExperimentRequest};
use pqbench::lz::LzRequest as LibLzRequest;
use pqbench::profile::ProfileRequest as LibProfileRequest;
use pqbench::stats;
use pqbench::table::get::Storage as LibStorage;
use pqbench::third_party::parquet::api::{read_sample, read_typed_sample};
use pqbench::third_party::parquet::api::{Sample as LibSample, TypedSample as LibTypedSample};
use pqbench::viz::MassRecord;
use pqbench::{
    Config as LibConfig, Dialect as LibDialect, Lake as LibLake, Layer as LibLayer,
    Workspace as LibWorkspace,
};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use pyo3::{Py, PyAny};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};

// ===== the client handles =====

/// The catalog dialect: Unity Catalog or Iceberg REST.
#[allow(clippy::upper_case_acronyms)]
#[pyclass(eq, eq_int, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum Dialect {
    UNITY,
    ICEBERG,
}

impl From<Dialect> for LibDialect {
    fn from(value: Dialect) -> Self {
        match value {
            Dialect::UNITY => Self::Unity,
            Dialect::ICEBERG => Self::Iceberg,
        }
    }
}

impl From<LibDialect> for Dialect {
    fn from(value: LibDialect) -> Self {
        match value {
            LibDialect::Unity => Self::UNITY,
            LibDialect::Iceberg => Self::ICEBERG,
        }
    }
}

/// How a sweep's samples reduce: the fastest pass, or their mean.
#[allow(clippy::upper_case_acronyms)]
#[pyclass(eq, eq_int, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    FASTEST,
    MEAN,
}

impl From<Mode> for stats::Mode {
    fn from(value: Mode) -> Self {
        match value {
            Mode::FASTEST => Self::Fastest,
            Mode::MEAN => Self::Mean,
        }
    }
}

/// What an experiment measures after a rewrite.
#[allow(clippy::upper_case_acronyms)]
#[pyclass(eq, eq_int, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum Aim {
    STORAGE,
    SKIPPING,
    ALL,
}

impl From<Aim> for LibAim {
    fn from(value: Aim) -> Self {
        match value {
            Aim::STORAGE => Self::Storage,
            Aim::SKIPPING => Self::Skipping,
            Aim::ALL => Self::All,
        }
    }
}

/// The endpoint, token, and dialect a `Workspace` runs under.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone)]
struct Config {
    endpoint: String,
    token: Option<String>,
    dialect: LibDialect,
}

#[pymethods]
impl Config {
    #[new]
    #[pyo3(signature = (endpoint, token=None, dialect=Dialect::UNITY))]
    fn new(endpoint: String, token: Option<String>, dialect: Dialect) -> Self {
        Self {
            endpoint,
            token,
            dialect: dialect.into(),
        }
    }

    #[getter]
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[getter]
    fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    #[getter]
    fn dialect(&self) -> Dialect {
        self.dialect.into()
    }

    fn __repr__(&self) -> String {
        format!(
            "Config(endpoint={:?}, token={:?}, dialect={:?})",
            self.endpoint,
            self.token,
            Dialect::from(self.dialect)
        )
    }
}

/// One configuration layer; `resolve` combines the set into a `Config`.
#[pyclass(skip_from_py_object)]
#[derive(Clone, Default)]
struct Layer {
    endpoint: Option<String>,
    token: Option<String>,
    dialect: Option<LibDialect>,
}

#[pymethods]
impl Layer {
    #[new]
    #[pyo3(signature = (endpoint=None, token=None, dialect=None))]
    fn new(endpoint: Option<String>, token: Option<String>, dialect: Option<Dialect>) -> Self {
        Self {
            endpoint,
            token,
            dialect: dialect.map(Into::into),
        }
    }

    fn resolve(&self) -> Config {
        LibLayer {
            endpoint: self.endpoint.clone(),
            token: self.token.clone(),
            dialect: self.dialect,
        }
        .resolve()
        .into()
    }

    fn __repr__(&self) -> String {
        format!(
            "Layer(endpoint={:?}, token={:?})",
            self.endpoint, self.token
        )
    }
}

impl From<LibConfig> for Config {
    fn from(value: LibConfig) -> Self {
        Self {
            endpoint: value.endpoint,
            token: value.token,
            dialect: value.dialect,
        }
    }
}

/// A catalog client: one HTTP pool for the endpoint.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct Workspace(LibWorkspace);

#[pymethods]
impl Workspace {
    #[new]
    fn new(config: &Config) -> PyResult<Self> {
        let config = LibConfig {
            endpoint: config.endpoint.clone(),
            token: config.token.clone(),
            dialect: config.dialect,
        };
        Ok(Self(LibWorkspace::new(config).map_err(runtime)?))
    }

    #[getter]
    fn config(&self) -> Config {
        self.0.config().clone().into()
    }

    fn __repr__(&self) -> String {
        format!("Workspace(config={:?})", self.config())
    }
}

/// An object-store client for a `Workspace`.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct Lake(LibLake);

#[pymethods]
impl Lake {
    #[new]
    fn new(workspace: &Workspace) -> Self {
        Self(LibLake::new(&workspace.0))
    }

    fn __repr__(&self) -> String {
        "Lake()".to_string()
    }
}

/// Where a table's metadata lives, for `table.get.read`.
#[pyclass(skip_from_py_object)]
#[derive(Clone, Default)]
struct Storage {
    location: Option<String>,
    env: BTreeMap<String, String>,
}

#[pymethods]
impl Storage {
    #[new]
    #[pyo3(signature = (location=None, env=None))]
    fn new(location: Option<String>, env: Option<BTreeMap<String, String>>) -> Self {
        Self {
            location,
            env: env.unwrap_or_default(),
        }
    }

    #[getter]
    fn location(&self) -> Option<&str> {
        self.location.as_deref()
    }

    #[getter]
    fn env(&self) -> BTreeMap<String, String> {
        self.env.clone()
    }
}

impl From<&Storage> for LibStorage {
    fn from(value: &Storage) -> Self {
        Self {
            location: value.location.clone(),
            env: value.env.clone(),
        }
    }
}

/// Arguments for `pqbench.lz.lz`.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct LzRequest {
    file: PathBuf,
    codec_specs: Vec<String>,
    samples: u32,
    warmup_iterations: u32,
    mode: stats::Mode,
}

#[pymethods]
impl LzRequest {
    #[new]
    #[pyo3(signature = (file, codec_specs=None, samples=10, warmup_iterations=3, mode=Mode::FASTEST))]
    fn new(
        file: PathBuf,
        codec_specs: Option<Vec<String>>,
        samples: u32,
        warmup_iterations: u32,
        mode: Mode,
    ) -> Self {
        Self {
            file,
            codec_specs: codec_specs.unwrap_or_default(),
            samples,
            warmup_iterations,
            mode: mode.into(),
        }
    }
}

impl From<&LzRequest> for LibLzRequest {
    fn from(value: &LzRequest) -> Self {
        Self {
            file: value.file.clone(),
            codec_specs: value.codec_specs.clone(),
            samples: value.samples,
            warmup_iterations: value.warmup_iterations,
            mode: value.mode,
        }
    }
}

/// Arguments for `pqbench.compression.compression`.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct CompressionRequest {
    file: PathBuf,
    codec_specs: Vec<String>,
    samples: u32,
    warmup_iterations: u32,
    mode: stats::Mode,
    per_column: bool,
}

#[pymethods]
impl CompressionRequest {
    #[new]
    #[pyo3(signature = (file, codec_specs=None, samples=10, warmup_iterations=3, mode=Mode::FASTEST, per_column=false))]
    fn new(
        file: PathBuf,
        codec_specs: Option<Vec<String>>,
        samples: u32,
        warmup_iterations: u32,
        mode: Mode,
        per_column: bool,
    ) -> Self {
        Self {
            file,
            codec_specs: codec_specs.unwrap_or_default(),
            samples,
            warmup_iterations,
            mode: mode.into(),
            per_column,
        }
    }
}

impl From<&CompressionRequest> for LibCompressionRequest {
    fn from(value: &CompressionRequest) -> Self {
        Self {
            file: value.file.clone(),
            codec_specs: value.codec_specs.clone(),
            samples: value.samples,
            warmup_iterations: value.warmup_iterations,
            mode: value.mode,
            per_column: value.per_column,
        }
    }
}

/// Arguments for `pqbench.bytemass.bytemass`.
#[pyclass(skip_from_py_object)]
#[derive(Clone, Default)]
struct BytemassRequest {
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

/// Arguments for `pqbench.profile.profile`.
#[pyclass(skip_from_py_object)]
#[derive(Clone, Default)]
struct ProfileRequest {
    columns: Vec<String>,
    top: u32,
}

#[pymethods]
impl ProfileRequest {
    #[new]
    #[pyo3(signature = (columns=None, top=5))]
    fn new(columns: Option<Vec<String>>, top: u32) -> Self {
        Self {
            columns: columns.unwrap_or_default(),
            top,
        }
    }
}

impl From<&ProfileRequest> for LibProfileRequest {
    fn from(value: &ProfileRequest) -> Self {
        let defaults = LibProfileRequest::default();
        Self {
            columns: value.columns.clone(),
            top: if value.top == 0 {
                defaults.top
            } else {
                value.top
            },
        }
    }
}

/// Arguments for `pqbench.experiment.experiment`.
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
struct ExperimentRequest {
    trials: Vec<String>,
    aim: LibAim,
}

#[pymethods]
impl ExperimentRequest {
    #[new]
    #[pyo3(signature = (trials=None, aim=Aim::ALL))]
    fn new(trials: Option<Vec<String>>, aim: Aim) -> Self {
        Self {
            trials: trials.unwrap_or_default(),
            aim: aim.into(),
        }
    }
}

impl From<&ExperimentRequest> for LibExperimentRequest {
    fn from(value: &ExperimentRequest) -> Self {
        Self {
            trials: value.trials.clone(),
            aim: value.aim,
        }
    }
}

/// A read sample for `pqbench.profile.profile`.
#[pyclass]
struct Sample(LibSample);

#[pymethods]
impl Sample {
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.0.columns.clone()
    }

    #[getter]
    fn row_count(&self) -> usize {
        self.0.rows.len()
    }
}

/// A typed sample for `pqbench.experiment.experiment`.
#[pyclass]
struct TypedSample(LibTypedSample);

#[pymethods]
impl TypedSample {
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.0
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    #[getter]
    fn row_count(&self) -> usize {
        self.0.rows.len()
    }
}

/// The library's rate limiter: per-kind minimum spacing between calls.
#[pyclass]
struct RateLimit(pqbench::ratelimit::RateLimit);

#[pymethods]
impl RateLimit {
    #[new]
    fn new(requests_per_second: f64) -> Self {
        Self(pqbench::ratelimit::RateLimit::new(requests_per_second))
    }

    /// How long to wait before the next call of `kind`, in seconds.
    fn delay(&mut self, kind: &str) -> f64 {
        self.0.delay(kind, std::time::Instant::now()).as_secs_f64()
    }
}

/// Whether a table is readable outside Databricks compute, and why not.
#[pyclass]
struct Eligibility {
    eligible: bool,
    reason: Option<String>,
}

#[pymethods]
impl Eligibility {
    #[getter]
    fn eligible(&self) -> bool {
        self.eligible
    }

    #[getter]
    fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    fn __repr__(&self) -> String {
        match &self.reason {
            Some(reason) => format!("Eligibility(eligible=False, reason={reason:?})"),
            None => "Eligibility(eligible=True)".to_string(),
        }
    }
}

impl From<pqbench::credentials::check::Eligibility> for Eligibility {
    fn from(value: pqbench::credentials::check::Eligibility) -> Self {
        use pqbench::credentials::check::Eligibility as Lib;
        match value {
            Lib::Eligible => Self {
                eligible: true,
                reason: None,
            },
            Lib::Ineligible(reason) => Self {
                eligible: false,
                reason: Some(
                    match reason {
                        pqbench::credentials::check::Reason::SystemTable => "system-table",
                        pqbench::credentials::check::Reason::NotATable => "not-a-table",
                        pqbench::credentials::check::Reason::NoExternalRead => "no-external-read",
                    }
                    .to_string(),
                ),
            },
        }
    }
}

// ===== the walk =====

#[pyfunction]
#[pyo3(name = "read")]
fn metastore_read(py: Python<'_>, workspace: PyRef<'_, Workspace>) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::metastore::get::read(&workspace)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "read")]
fn catalog_read(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::catalog::get::read(&workspace, &catalog)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "list")]
fn catalog_list(py: Python<'_>, workspace: PyRef<'_, Workspace>) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::catalog::ls::list(&workspace)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "read")]
fn schema_read(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
    schema: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::schema::get::read(&workspace, &catalog, &schema)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "list")]
fn schema_list(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::schema::ls::list(&workspace, &catalog)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "read")]
fn table_read(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    lake: PyRef<'_, Lake>,
    catalog: String,
    schema: String,
    table: String,
    storage: &Storage,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    let lake = lake.0.clone();
    let storage = LibStorage::from(storage);
    records(py, async move {
        pqbench::table::get::read(&workspace, &lake, &catalog, &schema, &table, &storage)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "list")]
fn table_list(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
    schema: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::table::ls::list(&workspace, &catalog, &schema)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "list")]
fn partition_list(
    py: Python<'_>,
    lake: PyRef<'_, Lake>,
    table: String,
    uri: String,
    env: BTreeMap<String, String>,
    window_millis: i64,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    stream_listing(py, async move {
        pqbench::partition::ls::list(&lake, &table, &uri, &env, window_millis).await
    })
}

#[pyfunction]
#[pyo3(name = "list")]
fn file_list(
    py: Python<'_>,
    lake: PyRef<'_, Lake>,
    uri: String,
    env: BTreeMap<String, String>,
    versions: Vec<u64>,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    stream_listing(py, async move {
        pqbench::file::ls::list(&lake, &uri, &env, &versions).await
    })
}

#[pyfunction]
#[pyo3(name = "check")]
fn credentials_check(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
    schema: String,
    table: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    drive(py, async move {
        let eligibility = pqbench::credentials::check::check(&workspace, &catalog, &schema, &table)
            .await
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        Python::attach(|py| Ok(Py::new(py, Eligibility::from(eligibility))?.into_any()))
    })
}

#[pyfunction]
#[pyo3(name = "vend_unity")]
fn credentials_vend_unity(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    catalog: String,
    schema: String,
    table: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::credentials::get::vend_unity(&workspace, &catalog, &schema, &table)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "vend_iceberg")]
fn credentials_vend_iceberg(
    py: Python<'_>,
    workspace: PyRef<'_, Workspace>,
    schema: String,
    table: String,
) -> PyResult<Py<PyAny>> {
    let workspace = workspace.0.clone();
    records(py, async move {
        pqbench::credentials::get::vend_iceberg(&workspace, &schema, &table)
            .await
            .map_err(|error| error.to_string())
    })
}

// ===== the analysis commands =====

#[pyfunction]
fn lz(py: Python<'_>, request: &LzRequest) -> PyResult<Py<PyAny>> {
    let request = LibLzRequest::from(request);
    let report = py.detach(|| pqbench::lz::lz(&request)).map_err(runtime)?;
    dumps(py, &report)
}

#[pyfunction]
fn compression(py: Python<'_>, request: &CompressionRequest) -> PyResult<Py<PyAny>> {
    let request = LibCompressionRequest::from(request);
    let report = py
        .detach(|| pqbench::compression::compression(&request))
        .map_err(runtime)?;
    dumps(py, &report)
}

#[pyfunction]
fn bytemass(
    py: Python<'_>,
    lake: PyRef<'_, Lake>,
    request: &BytemassRequest,
) -> PyResult<Py<PyAny>> {
    let lake = lake.0.clone();
    let request = LibBytemassRequest::from(request);
    records(py, async move {
        pqbench::bytemass::bytemass(&lake, &request)
            .await
            .map_err(|error| error.to_string())
    })
}

#[pyfunction]
#[pyo3(name = "read_sample")]
fn read_sample_rows(py: Python<'_>, path: PathBuf, max_rows: Option<usize>) -> PyResult<Py<PyAny>> {
    let sample = py
        .detach(|| read_sample(&path, max_rows))
        .map_err(runtime)?;
    Ok(Py::new(py, Sample(sample))?.into_any())
}

#[pyfunction]
#[pyo3(name = "read_typed_sample")]
fn read_typed_sample_rows(
    py: Python<'_>,
    path: PathBuf,
    max_rows: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let sample = py
        .detach(|| read_typed_sample(&path, max_rows))
        .map_err(runtime)?;
    Ok(Py::new(py, TypedSample(sample))?.into_any())
}

#[pyfunction]
fn profile(py: Python<'_>, sample: &Sample, request: &ProfileRequest) -> PyResult<Py<PyAny>> {
    let request = LibProfileRequest::from(request);
    let profile = py
        .detach(|| pqbench::profile::profile(&sample.0, &request))
        .map_err(runtime)?;
    dumps(py, &profile)
}

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

#[pyfunction]
#[pyo3(signature = (left, right, depth))]
fn compare(
    py: Python<'_>,
    left: Bound<'_, PyAny>,
    right: Bound<'_, PyAny>,
    depth: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let left = rows(&py_records(&left)?)?;
    let right = rows(&py_records(&right)?)?;
    let deltas = pqbench::diff::compare(&left, &right, depth).map_err(runtime)?;
    dumps(py, &deltas)
}

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

// ===== the stream iterator =====

/// A lazily-consumed stream of JSON values, driven on the shared runtime.
#[pyclass]
struct ValueIterator {
    rx: Arc<Mutex<mpsc::Receiver<Result<Value, String>>>>,
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
                Some(Ok(value)) => Python::attach(|py| value_to_py(py, &value)),
                Some(Err(error)) => Err(PyRuntimeError::new_err(error)),
            }
        })
    }
}

// ===== helpers =====

/// Drive a library stream on the shared runtime, as the Python async iterator.
fn stream_listing<F, S, T, E>(py: Python<'_>, source: F) -> PyResult<Py<PyAny>>
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
                Ok(record) => serde_json::to_value(&record).map_err(|error| error.to_string()),
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
fn iterator(py: Python<'_>, rx: mpsc::Receiver<Result<Value, String>>) -> PyResult<Py<PyAny>> {
    Ok(Py::new(
        py,
        ValueIterator {
            rx: Arc::new(Mutex::new(rx)),
        },
    )?
    .into_any())
}

/// Drive a future on the shared runtime and return its awaitable.
fn drive(
    py: Python<'_>,
    future: impl Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
) -> PyResult<Py<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, future).map(Bound::unbind)
}

/// Drive a future whose value serializes, and return the awaitable.
fn records<T>(
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

fn runtime(error: impl ToString) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn value_error(error: serde_json::Error) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Mass rows from their JSON records.
fn rows(records: &[Value]) -> PyResult<Vec<pqbench::bytemass::MassRow>> {
    records
        .iter()
        .map(|value| serde_json::from_value(value.clone()).map_err(value_error))
        .collect()
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

/// A Python value as JSON, via `json.dumps`.
fn py_to_value(object: &Bound<'_, PyAny>) -> PyResult<Value> {
    let json = object.py().import("json")?;
    let text: String = json.call_method1("dumps", (object,))?.extract()?;
    serde_json::from_str(&text).map_err(value_error)
}

/// The items of a Python list (or one object) as JSON values.
fn py_records(object: &Bound<'_, PyAny>) -> PyResult<Vec<Value>> {
    Ok(match py_to_value(object)? {
        Value::Array(items) => items,
        other => vec![other],
    })
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

fn dumps(py: Python<'_>, value: &impl Serialize) -> PyResult<Py<PyAny>> {
    let value = serde_json::to_value(value).map_err(value_error)?;
    value_to_py(py, &value)
}

fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
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
                    .unwrap_or_default()
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

// ===== the module =====

/// Make a child module and hang it under `parent`.
fn submodule<'py>(
    py: Python<'py>,
    parent: &Bound<'py, PyModule>,
    name: &str,
) -> PyResult<Bound<'py, PyModule>> {
    let module = PyModule::new(py, name)?;
    parent.add_submodule(&module)?;
    Ok(module)
}

/// Make the child importable by its dotted path.
fn register(py: Python<'_>, name: &str, module: &Bound<'_, PyModule>) -> PyResult<()> {
    py.import("sys")?.getattr("modules")?.set_item(name, module)
}

#[pymodule]
fn native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // pqbench reads tables from outside Databricks with explicit credentials
    // (vended, or on the source); it never relies on EC2 instance metadata.
    if std::env::var_os("AWS_EC2_METADATA_DISABLED").is_none() {
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
    let py = module.py();
    // One shared tokio runtime backs every awaitable; started at import so the
    // first call does not pay to build it.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    pyo3_async_runtimes::tokio::init(builder);

    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_class::<Dialect>()?;
    module.add_class::<Mode>()?;
    module.add_class::<Aim>()?;
    module.add_class::<Config>()?;
    module.add_class::<Layer>()?;
    module.add_class::<Workspace>()?;
    module.add_class::<Lake>()?;
    module.add_class::<Storage>()?;
    module.add_class::<LzRequest>()?;
    module.add_class::<CompressionRequest>()?;
    module.add_class::<BytemassRequest>()?;
    module.add_class::<ProfileRequest>()?;
    module.add_class::<ExperimentRequest>()?;
    module.add_class::<Sample>()?;
    module.add_class::<TypedSample>()?;
    module.add_class::<RateLimit>()?;
    module.add_class::<Eligibility>()?;
    module.add_class::<ValueIterator>()?;

    let metastore = submodule(py, module, "metastore")?;
    let get = submodule(py, &metastore, "get")?;
    get.add_function(wrap_pyfunction!(metastore_read, &get)?)?;
    register(py, "pqbench.native.metastore", &metastore)?;
    register(py, "pqbench.native.metastore.get", &get)?;

    let catalog = submodule(py, module, "catalog")?;
    let get = submodule(py, &catalog, "get")?;
    get.add_function(wrap_pyfunction!(catalog_read, &get)?)?;
    let ls = submodule(py, &catalog, "ls")?;
    ls.add_function(wrap_pyfunction!(catalog_list, &ls)?)?;
    register(py, "pqbench.native.catalog", &catalog)?;
    register(py, "pqbench.native.catalog.get", &get)?;
    register(py, "pqbench.native.catalog.ls", &ls)?;

    let schema = submodule(py, module, "schema")?;
    let get = submodule(py, &schema, "get")?;
    get.add_function(wrap_pyfunction!(schema_read, &get)?)?;
    let ls = submodule(py, &schema, "ls")?;
    ls.add_function(wrap_pyfunction!(schema_list, &ls)?)?;
    register(py, "pqbench.native.schema", &schema)?;
    register(py, "pqbench.native.schema.get", &get)?;
    register(py, "pqbench.native.schema.ls", &ls)?;

    let table = submodule(py, module, "table")?;
    let get = submodule(py, &table, "get")?;
    get.add_function(wrap_pyfunction!(table_read, &get)?)?;
    let ls = submodule(py, &table, "ls")?;
    ls.add_function(wrap_pyfunction!(table_list, &ls)?)?;
    register(py, "pqbench.native.table", &table)?;
    register(py, "pqbench.native.table.get", &get)?;
    register(py, "pqbench.native.table.ls", &ls)?;

    let partition = submodule(py, module, "partition")?;
    let ls = submodule(py, &partition, "ls")?;
    ls.add_function(wrap_pyfunction!(partition_list, &ls)?)?;
    register(py, "pqbench.native.partition", &partition)?;
    register(py, "pqbench.native.partition.ls", &ls)?;

    let file = submodule(py, module, "file")?;
    let ls = submodule(py, &file, "ls")?;
    ls.add_function(wrap_pyfunction!(file_list, &ls)?)?;
    register(py, "pqbench.native.file", &file)?;
    register(py, "pqbench.native.file.ls", &ls)?;

    let credentials = submodule(py, module, "credentials")?;
    let check = submodule(py, &credentials, "check")?;
    check.add_function(wrap_pyfunction!(credentials_check, &check)?)?;
    let get = submodule(py, &credentials, "get")?;
    get.add_function(wrap_pyfunction!(credentials_vend_unity, &get)?)?;
    get.add_function(wrap_pyfunction!(credentials_vend_iceberg, &get)?)?;
    register(py, "pqbench.native.credentials", &credentials)?;
    register(py, "pqbench.native.credentials.check", &check)?;
    register(py, "pqbench.native.credentials.get", &get)?;

    let m_lz = submodule(py, module, "lz")?;
    m_lz.add_function(wrap_pyfunction!(lz, &m_lz)?)?;
    register(py, "pqbench.native.lz", &m_lz)?;

    let m_compression = submodule(py, module, "compression")?;
    m_compression.add_function(wrap_pyfunction!(compression, &m_compression)?)?;
    register(py, "pqbench.native.compression", &m_compression)?;

    let m_bytemass = submodule(py, module, "bytemass")?;
    m_bytemass.add_function(wrap_pyfunction!(bytemass, &m_bytemass)?)?;
    register(py, "pqbench.native.bytemass", &m_bytemass)?;

    let m_profile = submodule(py, module, "profile")?;
    m_profile.add_function(wrap_pyfunction!(profile, &m_profile)?)?;
    register(py, "pqbench.native.profile", &m_profile)?;

    let m_experiment = submodule(py, module, "experiment")?;
    m_experiment.add_function(wrap_pyfunction!(experiment, &m_experiment)?)?;
    register(py, "pqbench.native.experiment", &m_experiment)?;

    let m_diff = submodule(py, module, "diff")?;
    m_diff.add_function(wrap_pyfunction!(compare, &m_diff)?)?;
    register(py, "pqbench.native.diff", &m_diff)?;

    let m_ratelimit = submodule(py, module, "ratelimit")?;
    m_ratelimit.add_class::<RateLimit>()?;
    register(py, "pqbench.native.ratelimit", &m_ratelimit)?;

    let m_skill = submodule(py, module, "skill")?;
    m_skill.add_function(wrap_pyfunction!(skills, &m_skill)?)?;
    m_skill.add_function(wrap_pyfunction!(skill, &m_skill)?)?;
    m_skill.add_function(wrap_pyfunction!(document, &m_skill)?)?;
    register(py, "pqbench.native.skill", &m_skill)?;

    let m_viz = submodule(py, module, "viz")?;
    m_viz.add_function(wrap_pyfunction!(write_report, &m_viz)?)?;
    register(py, "pqbench.native.viz", &m_viz)?;

    let third_party = submodule(py, module, "third_party")?;
    let parquet = submodule(py, &third_party, "parquet")?;
    parquet.add_function(wrap_pyfunction!(read_sample_rows, &parquet)?)?;
    parquet.add_function(wrap_pyfunction!(read_typed_sample_rows, &parquet)?)?;
    register(py, "pqbench.native.third_party", &third_party)?;
    register(py, "pqbench.native.third_party.parquet", &parquet)?;

    Ok(())
}
