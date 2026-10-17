//! `ratelimit`: the library token bucket, exposed so a caller can pace.

use crate::prelude::*;

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

pub(crate) fn register(py: Python<'_>, root: &Bound<'_, PyModule>) -> PyResult<()> {
    root.add_class::<RateLimit>()?;
    let module = crate::submodule(py, root, "ratelimit")?;
    module.add_class::<RateLimit>()?;
    crate::expose(py, "pqbench.native.ratelimit", &module)?;
    Ok(())
}
