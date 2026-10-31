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

/// The probability at which a file of `size_bytes` is kept at `rate`.
///
/// `rate` is the probability at [`REFERENCE_BYTES`]; larger files are more
/// likely, and `rate = 1` keeps everything. The caller validates `rate`: it
/// must be finite and in `(0, 1]`.
fn probability(size_bytes: u64, rate: f64) -> f64 {
    if rate >= 1.0 {
        return 1.0;
    }
    (rate * size_bytes as f64 / REFERENCE_BYTES as f64).min(1.0)
}

/// Keep one table file at `rate`, returning the probability it was kept with.
///
/// The decision hashes the record's name, so the same record always makes the
/// same one. `None` when the file is dropped.
///
/// A record that already carries a `sample_rate` — an earlier `sample files`
/// stage — composes with this one by keeping the smaller probability: both
/// stages threshold the same hash of the name, so nested thresholds make the
/// effective probability the minimum.
///
/// # Errors
/// Fails when `rate` is outside `(0, 1]`, when the record's own `sample_rate`
/// is outside it, when `rate` underflows to zero for the file's size, or when
/// the record has no positive `size_bytes`: a sample by size cannot place a
/// file it cannot size.
pub fn select(file: &v1::TableFile, rate: f64) -> Result<Option<f64>, Error> {
    if !(rate > 0.0 && rate <= 1.0) {
        return Err(Error::Rate(rate));
    }
    let size_bytes = file
        .size_bytes
        .filter(|size_bytes| *size_bytes > 0)
        .ok_or_else(|| Error::UnknownSize(file.name.clone()))?;
    let drawn = probability(size_bytes, rate);
    if drawn <= 0.0 {
        return Err(Error::Underflow(rate));
    }
    let probability = match file.sample_rate {
        Some(previous) if previous > 0.0 && previous <= 1.0 => previous.min(drawn),
        Some(previous) => return Err(Error::Sampled(previous)),
        None => drawn,
    };
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
    /// A record already carries a `sample_rate` outside `(0, 1]`.
    Sampled(f64),
    /// The rate underflows to zero for this file's size.
    Underflow(f64),
    /// A record names no positive size.
    UnknownSize(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rate(rate) => write!(f, "sample rate must be in (0, 1], not {rate}"),
            Self::Sampled(rate) => {
                write!(f, "the record's sample_rate must be in (0, 1], not {rate}")
            }
            Self::Underflow(rate) => write!(
                f,
                "sample rate {rate} rounds to zero at this file size; no file can be kept"
            ),
            Self::UnknownSize(name) => write!(
                f,
                "sampling needs a positive file size: {name} has no positive size"
            ),
        }
    }
}

impl std::error::Error for Error {}
