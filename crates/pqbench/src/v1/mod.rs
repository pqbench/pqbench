//! The pqbench wire types, and the one home for resource names.
//!
//! Each area of the model lives in its own module, so a change to one
//! touches one file.
//!
//! One definition per resource and value, so a command and the CLI share a
//! type instead of each declaring its own. The CLI forwards these as-is (it
//! renders the human table for them); the library's commands return them.
//!
//! These are ordinary Rust types first: a value that can be absent is an
//! `Option`, a quantity is unsigned, an enum has no "unspecified" sentinel
//! (absence is `None`), and a collection is a `Vec`/`BTreeMap`. The modelling
//! rules are `docs/concepts/data-modelling.md` and the wire contract is
//! `docs/concepts/v1.proto`; where a wire idiom (a zero-value variant, an
//! `int64`, a prefixed enum value) is not the Rust idiom, Rust wins and the
//! wire keeps the spelling via `serde`.
//!
//! [`name`] is the one place a name format lives, so changing it (say, to a
//! URN) is a one-module change.

mod analysis;
mod catalog;
mod credentials;
mod experiment;
mod file;
pub mod json;
mod log;
pub mod name;
mod table;

pub use analysis::{Bytemass, ColumnProfile, CompressionReport};
pub use catalog::{Catalog, CatalogType, Metastore, Schema};
pub use credentials::{Credentials, EligibilityReason, ReadEligibility};
pub use experiment::ExperimentTrial;
pub use file::{Blob, Column, TableFile};
pub use json::FromJson;
pub use log::{Commit, FileStats, LogAction};
pub use table::{DeclaredColumn, Partition, Table, TableFormat};

/// A level's parent record: a resource read on standard input, and the noun a
/// message calls it.
pub trait Parent: json::FromJson {
    /// The stream's noun, for messages.
    const NOUN: &'static str;

    /// The record's resource name: what a failure is reported under.
    fn name(&self) -> &str;
}
