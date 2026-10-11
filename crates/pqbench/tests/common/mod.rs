//! Shared fixtures for the library's integration tests.

use std::sync::OnceLock;

use pqbench::{Lake, Workspace};

/// One [`Lake`] for the whole test binary. The resources are a process-wide
/// pool, as in the CLI; a test that reads only local storage needs no config.
pub fn lake() -> &'static Lake {
    static LAKE: OnceLock<Lake> = OnceLock::new();
    LAKE.get_or_init(|| {
        let workspace = Workspace::new(Default::default()).expect("workspace");
        Lake::new(&workspace)
    })
}
