//! The typed requests: what a caller passes to a command.

use crate::handles::{Aim, Mode};
use crate::prelude::*;

/// Where a table's metadata lives, for `table.get.read`.
///
/// `env` is the `AWS_*` options to read with. `credentials` is a vended lease
/// as a ref carries it (`access_key_id`, `secret_access_key`,
/// `session_token`), converted to those options, so
/// `Storage(credentials=ref["credentials"])` needs no mapping by hand.
#[pyclass(skip_from_py_object)]
#[derive(Clone, Default)]
pub(crate) struct Storage {
    location: Option<String>,
    env: BTreeMap<String, String>,
}
#[pymethods]
impl Storage {
    #[new]
    #[pyo3(signature = (location=None, env=None, credentials=None))]
    fn new(
        location: Option<String>,
        env: Option<BTreeMap<String, String>>,
        credentials: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let mut env = env.unwrap_or_default();
        if let Some(credentials) = credentials {
            let value = crate::marshal::py_to_value(&credentials)?;
            let lease: pqbench::v1::Credentials =
                serde_json::from_value(value).map_err(crate::marshal::value_error)?;
            env.extend(lease.to_env());
        }
        Ok(Self { location, env })
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
pub(crate) struct LzRequest {
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
pub(crate) struct CompressionRequest {
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
/// Arguments for `pqbench.profile.profile`.
#[pyclass(skip_from_py_object)]
#[derive(Clone, Default)]
pub(crate) struct ProfileRequest {
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
pub(crate) struct ExperimentRequest {
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

pub(crate) fn register(root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<Storage>()?;
    root.add_class::<LzRequest>()?;
    root.add_class::<CompressionRequest>()?;
    root.add_class::<BytemassRequest>()?;
    root.add_class::<ProfileRequest>()?;
    root.add_class::<ExperimentRequest>()?;
    Ok(())
}
