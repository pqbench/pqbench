//! Codec wiring + lzbench-style single-buffer benchmark core.
//!
//! Each codec lives in its own file (`snappy.rs`, `zstd.rs`, `lz4.rs`) as a type
//! implementing `CodecImpl`, with its own tests. The `Codec` enum is the closed
//! Parquet codec set and dispatches to the per-codec impls. `bench_buffer` is the
//! lzbench core loop reimplemented over the trait: warmup, iterate until the
//! min-time floor, keep the fastest time, verify round-trip.

mod gzip;
mod lz4;
mod snappy;
mod zstd;

pub use gzip::Gzip;
pub use lz4::Lz4;
pub use snappy::Snappy;
pub use zstd::Zstd;

use std::fmt;
use std::hint::black_box;
use std::time::{Duration, Instant};

use serde::Serialize;

/// Errors from the codec layer.
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Level {
        codec: &'static str,
        level: u8,
        first_level: u32,
        last_level: u32,
    },
    Codec(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Level {
                codec,
                level,
                first_level,
                last_level,
            } => {
                write!(
                    f,
                    "level {level} out of range for {codec}: {first_level}..={last_level}"
                )
            }
            Error::Codec(e) => write!(f, "codec error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// The closed set of Parquet compression codecs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    Snappy,
    Zstd,
    Lz4,
    Gzip,
}

impl Codec {
    /// The concrete implementation backing this variant (the registry).
    pub fn implementation(&self) -> &'static dyn CodecImpl {
        match self {
            Codec::Snappy => &Snappy,
            Codec::Zstd => &Zstd,
            Codec::Lz4 => &Lz4,
            Codec::Gzip => &Gzip,
        }
    }

    /// Parse a CLI codec name. `None` for unknown names.
    pub fn from_name(name: &str) -> Option<Codec> {
        Codec::all().find(|c| c.name() == name)
    }

    /// All wired codecs, in registry order.
    pub fn all() -> impl Iterator<Item = Codec> {
        [Codec::Snappy, Codec::Zstd, Codec::Lz4, Codec::Gzip].into_iter()
    }
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A codec's valid compression levels, inclusive (AIP-145: levels are a
/// colloquially-inclusive range, so `first_`/`last_` rather than half-open).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelRange {
    pub first_level: u32,
    pub last_level: u32,
}

/// The uniform interface every codec implements.
pub trait CodecImpl {
    fn name(&self) -> &'static str;
    fn level_range(&self) -> LevelRange;
    fn compress(&self, level: u8, src: &[u8]) -> Result<Vec<u8>, Error>;
    fn decompress(&self, level: u8, src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error>;
}

impl CodecImpl for Codec {
    fn name(&self) -> &'static str {
        self.implementation().name()
    }

    fn level_range(&self) -> LevelRange {
        self.implementation().level_range()
    }

    fn compress(&self, level: u8, src: &[u8]) -> Result<Vec<u8>, Error> {
        check_level(self, level)?;
        self.implementation().compress(level, src)
    }

    fn decompress(&self, level: u8, src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
        check_level(self, level)?;
        self.implementation().decompress(level, src, out_cap)
    }
}

fn check_level(codec: &Codec, level: u8) -> Result<(), Error> {
    let range = codec.level_range();
    if !(range.first_level..=range.last_level).contains(&(level as u32)) {
        return Err(Error::Level {
            codec: codec.name(),
            level,
            first_level: range.first_level,
            last_level: range.last_level,
        });
    }
    Ok(())
}

/// Raw per-pass timings for one codec×level over a set of page buffers. The
/// measurement core: it only records times and sizes — warmup and mode are
/// upstream (analytics) decisions.
pub struct PageSamples {
    /// Compressed size per page, in page order (exact, from the verify pass).
    pub page_compressed_sizes: Vec<usize>,
    /// One timed full-set compress pass per element, in order.
    pub compress_durations: Vec<Duration>,
    /// One timed full-set decompress pass per element, in order.
    pub decompress_durations: Vec<Duration>,
    /// Per page: one timed compress pass per element, in pass order.
    pub page_compress_durations: Vec<Vec<Duration>>,
    /// Per page: one timed decompress pass per element, in pass order.
    pub page_decompress_durations: Vec<Vec<Duration>>,
}

/// Raw per-pass timings over a single buffer.
pub struct BufferSamples {
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
    pub compress_durations: Vec<Duration>,
    pub decompress_durations: Vec<Duration>,
}

/// Benchmark a set of page buffers: verify each round-trip once (untimed),
/// then record `passes` timed full-set compress and decompress passes.
pub fn bench_pages(
    codec: Codec,
    level: u8,
    pages: &[&[u8]],
    passes: u32,
) -> Result<PageSamples, Error> {
    let mut compressed: Vec<Vec<u8>> = Vec::with_capacity(pages.len());
    let mut page_compressed_sizes = Vec::with_capacity(pages.len());
    for p in pages {
        let c = codec.compress(level, p)?;
        let back = codec.decompress(level, &c, p.len())?;
        if back != *p {
            return Err(Error::Codec("round-trip mismatch".to_string()));
        }
        page_compressed_sizes.push(c.len());
        compressed.push(c);
    }

    let mut compress_durations = Vec::with_capacity(passes as usize);
    let mut decompress_durations = Vec::with_capacity(passes as usize);
    let mut page_compress_durations = vec![Vec::with_capacity(passes as usize); pages.len()];
    let mut page_decompress_durations = vec![Vec::with_capacity(passes as usize); pages.len()];
    for _ in 0..passes {
        let pass = Instant::now();
        for (i, p) in pages.iter().enumerate() {
            let t = Instant::now();
            black_box(codec.compress(level, p)?);
            page_compress_durations[i].push(t.elapsed());
        }
        compress_durations.push(pass.elapsed());

        let pass = Instant::now();
        for (i, (p, c)) in pages.iter().zip(&compressed).enumerate() {
            let t = Instant::now();
            black_box(codec.decompress(level, c, p.len())?);
            page_decompress_durations[i].push(t.elapsed());
        }
        decompress_durations.push(pass.elapsed());
    }

    Ok(PageSamples {
        page_compressed_sizes,
        compress_durations,
        decompress_durations,
        page_compress_durations,
        page_decompress_durations,
    })
}

/// Benchmark a single byte buffer: a one-page wrapper over [`bench_pages`].
pub fn bench_buffer(
    codec: Codec,
    level: u8,
    src: &[u8],
    passes: u32,
) -> Result<BufferSamples, Error> {
    let r = bench_pages(codec, level, std::slice::from_ref(&src), passes)?;
    Ok(BufferSamples {
        compressed_bytes: r.page_compressed_sizes[0],
        uncompressed_bytes: src.len(),
        compress_durations: r.compress_durations,
        decompress_durations: r.decompress_durations,
    })
}

/// A deterministic 1 MB compressible sample used by the codec tests. Generated
/// in code (no third-party data), so it carries no attribution requirements.
/// Repetitive but not constant, so every codec compresses it to fewer bytes.
#[cfg(test)]
pub(crate) fn one_mb_sample() -> Vec<u8> {
    const LINE: &str = "the quick brown fox jumps over the lazy dog 0123456789 ";
    let mut out = Vec::with_capacity(1024 * 1024);
    let mut i = 0u32;
    while out.len() < 1024 * 1024 {
        out.extend_from_slice(format!("{LINE}{i:08}\n").as_bytes());
        i += 1;
    }
    out.truncate(1024 * 1024);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_name_round_trips() {
        assert_eq!(Codec::from_name("snappy"), Some(Codec::Snappy));
        assert_eq!(Codec::from_name("zstd"), Some(Codec::Zstd));
        assert_eq!(Codec::from_name("lz4"), Some(Codec::Lz4));
        assert_eq!(Codec::from_name("gzip"), Some(Codec::Gzip));
        assert_eq!(Codec::from_name("lzo"), None);
    }

    #[test]
    fn names_are_stable() {
        assert_eq!(Codec::Snappy.name(), "snappy");
        assert_eq!(Codec::Zstd.name(), "zstd");
        assert_eq!(Codec::Lz4.name(), "lz4");
    }

    #[test]
    fn enum_dispatch_round_trips_all_codecs() {
        let src = one_mb_sample();
        for codec in Codec::all() {
            let level = codec.level_range().first_level as u8;
            let c = codec.compress(level, &src).unwrap();
            let d = codec.decompress(level, &c, src.len()).unwrap();
            assert_eq!(d, src, "{codec} round-trip mismatch");
        }
    }

    #[test]
    fn bench_buffer_records_raw_samples() {
        let src = one_mb_sample();
        let r = bench_buffer(Codec::Snappy, 1, &src, 3).unwrap();
        assert_eq!(r.uncompressed_bytes, src.len());
        assert!(r.compressed_bytes < r.uncompressed_bytes);
        assert_eq!(r.compress_durations.len(), 3);
        assert_eq!(r.decompress_durations.len(), 3);
        assert!(r.compress_durations.iter().all(|t| *t > Duration::ZERO));
    }

    #[test]
    fn out_of_range_level_is_an_error() {
        assert!(matches!(
            Codec::Zstd.compress(30, b"x"),
            Err(Error::Level { .. })
        ));
        assert!(matches!(
            Codec::Zstd.compress(0, b"x"),
            Err(Error::Level { .. })
        ));
    }
}
