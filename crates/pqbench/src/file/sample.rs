//! `file::sample`: keep a rate of a table's files, by size.
//!
//! [`select`] draws each record independently, so the stage streams: it needs
//! the file's size and name, never the table's total. The probability grows
//! linearly with the file's size — a footer costs the same whatever the file
//! weighs, so the bytes are where the footers belong — and is capped at one.
//! The returned probability is the record's `sample_rate`: a sum over the kept
//! records divides each term by it.

use std::fmt;

use crate::v1;

/// The size at which a file is kept with probability `rate`: 256 MiB, the
/// target most Delta tables are compacted to.
pub const REFERENCE_BYTES: u64 = 256 * 1024 * 1024;

/// The inclusion probability for a file of `size_bytes` at `rate`.
///
/// `rate` is the probability at [`REFERENCE_BYTES`]; larger files are more
/// likely, and `rate = 1` keeps everything.
#[must_use]
pub fn inclusion_probability(size_bytes: u64, rate: f64) -> f64 {
    if rate >= 1.0 {
        return 1.0;
    }
    let scaled = rate * size_bytes as f64 / REFERENCE_BYTES as f64;
    scaled.min(1.0)
}

/// Keep one table file at `rate`, returning the probability it was kept with.
///
/// The decision hashes the record's name, so the same record always makes the
/// same one. `None` when the file is dropped.
///
/// # Errors
/// Fails when `rate` is outside `(0, 1]`, or when the record has no positive
/// `size_bytes`: a sample by size cannot place a file it cannot size.
pub fn select(file: &v1::TableFile, rate: f64) -> Result<Option<f64>, Error> {
    if !(rate > 0.0 && rate <= 1.0) {
        return Err(Error::Rate(rate));
    }
    let size_bytes = file
        .size_bytes
        .filter(|size_bytes| *size_bytes > 0)
        .ok_or_else(|| Error::UnknownSize(file.name.clone()))?;
    let probability = inclusion_probability(size_bytes, rate);
    Ok((hash_unit(&file.name) < probability).then_some(probability))
}

/// A value in `[0, 1)` from a name: FNV-1a, then the Murmur3 finalizer so
/// names that differ in one suffix byte still scatter. Stable across toolchain
/// versions, unlike the standard library's hasher.
fn hash_unit(name: &str) -> f64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^= hash >> 33;
    (hash >> 11) as f64 / (1_u64 << 53) as f64
}

/// Errors sampling a table's files.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The rate is outside `(0, 1]`.
    Rate(f64),
    /// A record names no positive size.
    UnknownSize(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rate(rate) => write!(f, "sample rate must be in (0, 1], not {rate}"),
            Self::UnknownSize(name) => {
                write!(f, "sampling needs a positive file size: {name} has none")
            }
        }
    }
}

impl std::error::Error for Error {}
