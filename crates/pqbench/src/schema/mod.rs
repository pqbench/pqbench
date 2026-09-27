//! The schema command: one schema's record and the tables in it.
//!
//! The schema is the entity between catalogs and tables. [`info`] reads its
//! record; [`ls`] lists the tables in it as `pqbench.table-ref` lines.

mod dialect;
pub mod info;
pub mod ls;

/// The catalog dialect a schema command speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    /// Unity REST: `/schemas`, `/tables`.
    Unity,
    /// Iceberg REST: `/namespaces…`; the endpoint already names the catalog
    /// base (`{root}/v1` or `{root}/v1/{prefix}`), so no config probe runs.
    Iceberg,
}
