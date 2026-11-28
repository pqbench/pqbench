//! JSON, isolated from the rest of pqbench.
//!
//! [`api`] is the surface pqbench parses and writes through; the private
//! `impl` module is the only file that names a JSON crate, and the only place a
//! feature selects one.
//!
//! Writing is `serde_json` either way — a SIMD parser parses, it does not
//! write. Parsing is `serde_json` without the `json-simd` feature and
//! `simd-json` with it: the same surface, a faster kernel that dispatches at
//! runtime to AVX2 + PCLMULQDQ, SSE4.2, or a portable stage 2, so a binary
//! built on one machine still runs on another.

pub mod api;
mod r#impl;

pub use api::{from_slice, from_slice_mut, from_str, to_vec, to_writer, Error};
