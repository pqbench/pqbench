//! LZ4 (raw block and frame formats via the `lz4` crate), isolated from the
//! rest of pqbench.
//!
//! [`api`] is the public surface; the private `impl` module is the only file
//! that names the `lz4` crate.

pub mod api;
mod r#impl;

pub use api::{compress, compress_frame, decompress, decompress_frame, Error};
