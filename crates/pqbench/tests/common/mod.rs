//! Shared fixtures for the library's integration tests.

use std::sync::OnceLock;

use pqbench::Context;

/// One [`Context`] for the whole test binary. The resources are a process-wide
/// pool, as in the CLI; a test that reads only local storage needs no config.
pub fn context() -> &'static Context {
    static CONTEXT: OnceLock<Context> = OnceLock::new();
    CONTEXT.get_or_init(|| Context::new(Default::default()).expect("context"))
}
