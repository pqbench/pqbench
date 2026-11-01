//! Every `pqbench-cli` integration test, as one Cargo test target.
//!
//! Cargo gives each `tests/*.rs` file its own target, and each target statically
//! links the whole dependency graph (pqbench, arrow, the AWS SDK). One target
//! links that graph once instead of once per file, which is what keeps
//! `make check` fast. `autotests = false` in `Cargo.toml` stops Cargo from also
//! building the individual files as targets.
//!
//! Run one file's tests with a module filter, for example
//! `cargo test -p pqbench-cli --all-features --test it -- bytemass_e2e`.

mod common;

#[path = "gen_support.rs"]
mod support;

#[path = "gen_all.rs"]
mod generated;

#[path = "bytemass_e2e.rs"]
mod bytemass_e2e;

#[path = "catalog.rs"]
mod catalog;

#[path = "compression_e2e.rs"]
mod compression_e2e;

#[path = "dbx_e2e.rs"]
mod dbx_e2e;

#[path = "diff.rs"]
mod diff;

#[path = "file.rs"]
mod file;

#[path = "format.rs"]
mod format;

#[path = "format_html.rs"]
mod format_html;

#[path = "help.rs"]
mod help;

#[path = "metastore.rs"]
mod metastore;

#[path = "piping.rs"]
mod piping;

#[path = "profile.rs"]
mod profile;

#[path = "ratelimit.rs"]
mod ratelimit;

#[path = "sample.rs"]
mod sample;

#[path = "schema.rs"]
mod schema;

#[path = "skill.rs"]
mod skill;

#[path = "stdin.rs"]
mod stdin;

#[path = "summarize.rs"]
mod summarize;

#[path = "table.rs"]
mod table;
