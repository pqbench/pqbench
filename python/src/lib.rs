//! The `pqbench` library, exposed to Python as `pqbench.native`.
//!
//! Every name is the library function of the same path, with the same
//! arguments: `pqbench.native.metastore.get.read(workspace)`,
//! `pqbench.native.catalog.list.list(workspace)`,
//! `pqbench.native.file.list.list(lake, uri, env, versions)`,
//! `pqbench.native.credentials.check.check(workspace, catalog, schema, table)`.
//!
//! The client handles — `Config`, `Layer`, `Workspace`, `Lake` — and the
//! request structs (`Storage`, `LzRequest`, `CompressionRequest`,
//! `BytemassRequest`, `ProfileRequest`, `ExperimentRequest`) are Python
//! classes, and so are the records: a model's record is its class, so
//! `record["uri"]`, `record.uri` and `record.asdict()` agree, and the deep
//! trees it embeds stay dicts. A collection the library streams is an async
//! iterator, a single value an awaitable.
//!
//! The binding is split the way the library is: one module per domain, each
//! owning its command and the function that registers it, so a domain can be
//! read and changed on its own. [`prelude`] holds the names they share, and
//! this file is only the module tree.

mod bytemass;
mod catalog;
mod compression;
mod credentials;
mod diff;
mod experiment;
mod file;
mod handles;
mod lz;
mod marshal;
mod metastore;
mod partition;
mod prelude;
mod profile;
mod ratelimit;
mod records;
mod requests;
mod sample;
mod schema;
mod skill;
mod table;
mod viz;

use pyo3::prelude::*;
use pyo3::types::PyModule;

/// Make a child module and hang it under `parent`.
pub(crate) fn submodule<'py>(
    py: Python<'py>,
    parent: &Bound<'py, PyModule>,
    name: &str,
) -> PyResult<Bound<'py, PyModule>> {
    let module = PyModule::new(py, name)?;
    parent.add_submodule(&module)?;
    Ok(module)
}

/// Make a child importable by its dotted path.
pub(crate) fn expose(py: Python<'_>, name: &str, module: &Bound<'_, PyModule>) -> PyResult<()> {
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
    handles::register(module)?;
    requests::register(module)?;
    ratelimit::register(py, module)?;
    marshal::register(module)?;
    records::register(module)?;
    sample::register(py, module)?;
    credentials::register(py, module)?;
    metastore::register(py, module)?;
    catalog::register(py, module)?;
    schema::register(py, module)?;
    table::register(py, module)?;
    partition::register(py, module)?;
    file::register(py, module)?;
    lz::register(py, module)?;
    compression::register(py, module)?;
    bytemass::register(py, module)?;
    profile::register(py, module)?;
    experiment::register(py, module)?;
    diff::register(py, module)?;
    skill::register(py, module)?;
    viz::register(py, module)?;
    Ok(())
}
