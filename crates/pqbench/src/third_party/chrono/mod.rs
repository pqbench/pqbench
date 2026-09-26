//! Date/time handling (via `chrono`), isolated from the rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names the `chrono` crate.

pub mod api;
mod r#impl;

pub use api::{format_instant, parse_instant, Error};
