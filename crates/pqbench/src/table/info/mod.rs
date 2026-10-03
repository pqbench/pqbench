//! `table info`: the storage path behind a table's record address.
//!
//! [`api`] is the public surface: one function, [`api::read`]. The private
//! `impl` module holds the URL shape and the HTTP transport.

pub mod api;
mod r#impl;

pub use api::{read, Error};
