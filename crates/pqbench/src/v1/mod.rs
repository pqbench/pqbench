//! The pqbench wire types, and the one home for resource names.
//!
//! Each resource lives in its own module, so a change to one touches one
//! file.
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

mod blob;
mod bytemass;
mod catalog;
mod catalog_type;
mod column;
mod column_profile;
mod commit;
mod compression_report;
mod credentials;
mod declared_column;
mod eligibility_reason;
mod experiment_trial;
mod file_stats;
mod log_action;
mod metastore;
pub mod name;
mod partition;
mod read_eligibility;
mod schema;
mod table;
mod table_file;
mod table_format;

pub use blob::Blob;
pub use bytemass::Bytemass;
pub use catalog::Catalog;
pub use catalog_type::CatalogType;
pub use column::Column;
pub use column_profile::ColumnProfile;
pub use commit::Commit;
pub use compression_report::CompressionReport;
pub use credentials::Credentials;
pub use declared_column::DeclaredColumn;
pub use eligibility_reason::EligibilityReason;
pub use experiment_trial::ExperimentTrial;
pub use file_stats::FileStats;
pub use log_action::LogAction;
pub use metastore::Metastore;
pub use partition::Partition;
pub use pqbench_resource::Resource;
pub use read_eligibility::ReadEligibility;
pub use schema::Schema;
pub use table::Table;
pub use table_file::TableFile;
pub use table_format::TableFormat;
