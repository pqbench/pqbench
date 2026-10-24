//! The client handles: what a caller constructs and every command takes.

use crate::marshal::runtime;
use crate::prelude::*;

use std::time::Duration;

/// How long one request may take: the same bound the CLI sets, so a Python
/// caller and a `pqbench` run time out alike.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// The catalog dialect: Unity Catalog or Iceberg REST.
#[allow(clippy::upper_case_acronyms)]
#[pyclass(eq, eq_int, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Dialect {
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
/// The endpoint, token, and dialect a `Workspace` runs under.
#[pyclass(skip_from_py_object)]
#[derive(Debug, Clone)]
pub(crate) struct Config {
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
pub(crate) struct Layer {
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
pub(crate) struct Workspace(pub(crate) LibWorkspace);

#[pymethods]
impl Workspace {
    #[new]
    fn new(config: &Config) -> PyResult<Self> {
        let config = LibConfig {
            endpoint: config.endpoint.clone(),
            token: config.token.clone(),
            dialect: config.dialect,
            request_timeout: Some(REQUEST_TIMEOUT),
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
pub(crate) struct Lake(pub(crate) LibLake);

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

pub(crate) fn register(root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<Dialect>()?;
    root.add_class::<Config>()?;
    root.add_class::<Layer>()?;
    root.add_class::<Workspace>()?;
    root.add_class::<Lake>()?;
    Ok(())
}
