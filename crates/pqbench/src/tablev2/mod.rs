//! The table command: one table's record.
//!
//! The table is the entity between schemas and partitions. [`info`] reads its
//! record: the catalog names the table and its location, and the table's own
//! metadata is read without files, so the cost is O(1) in files.

mod dialect;
pub mod info;

/// The catalog dialect a table command speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    /// Unity REST: `/tables`.
    Unity,
    /// Iceberg REST: `/namespaces…/tables`; the endpoint already names the
    /// catalog base (`{root}/v1` or `{root}/v1/{prefix}`), so no config probe
    /// runs.
    Iceberg,
}
