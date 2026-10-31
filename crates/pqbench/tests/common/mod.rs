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

/// The columns of the file at `uri`, measured through the shared lake.
#[allow(dead_code)]
pub async fn try_columns(uri: &str) -> Result<Vec<pqbench::v1::Column>, pqbench::bytemass::Error> {
    let blob = pqbench::v1::name::blob_name(uri);
    pqbench::bytemass::list_columns(lake(), &blob).await
}

/// The columns of the file at `uri`; panics when the file cannot be measured.
#[allow(dead_code)]
pub async fn columns(uri: &str) -> Vec<pqbench::v1::Column> {
    try_columns(uri).await.expect("a measurable file")
}
