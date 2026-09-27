//! The `reqwest` wrapper: one request in, one response out.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names `reqwest`.

pub mod api;
mod r#impl;

pub use api::{request, Error, Request, Response};
