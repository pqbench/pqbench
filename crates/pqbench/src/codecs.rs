//! Codec wiring + lzbench-style single-buffer benchmark core.
//!
//! The [`Codec`] enum is the closed Parquet codec set; its inherent `compress`
//! and `decompress` delegate to a domain-independent [`crate::third_party`]
//! wrapper (buffer in, buffer out, never a crate type). [`bench_pages`] is the
//! lzbench core loop: verify each round-trip, then time the passes.

use std::fmt;
use std::hint::black_box;
use std::time::{Duration, Instant};

use serde::Serialize;

/// Errors from the codec layer.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    Io(std::io::Error),
    Level {
        codec: &'static str,
        level: u8,
        first_level: u32,
        last_level: u32,
    },
    /// A compress/decompress round trip did not return the input.
    RoundTrip,
    /// The codec has no standalone frame format.
    Frame {
        codec: &'static str,
    },
    /// The codec backend failed.
    Codec(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(_) => write!(f, "codec i/o failed"),
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
            Error::RoundTrip => write!(f, "round-trip mismatch"),
            Error::Frame { codec } => write!(f, "{codec} has no frame format"),
            Error::Codec(_) => write!(f, "codec failed"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            Error::Codec(error) => Some(error.as_ref()),
            Error::Level { .. } | Error::RoundTrip | Error::Frame { .. } => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// Wrap a codec backend's error.
fn codec_error(e: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Codec(Box::new(e))
}

/// A codec's valid compression levels, inclusive (AIP-145: levels are a
/// colloquially-inclusive range, so `first_`/`last_` rather than half-open).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelRange {
    pub first_level: u32,
    pub last_level: u32,
}

/// The closed set of Parquet compression codecs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    Snappy,
    Zstd,
    Lz4,
    Gzip,
}

impl Codec {
    /// The codec's CLI and report name.
    pub fn name(&self) -> &'static str {
        match self {
            Codec::Snappy => "snappy",
            Codec::Zstd => "zstd",
            Codec::Lz4 => "lz4",
            Codec::Gzip => "gzip",
        }
    }

    /// The codec's valid levels, inclusive.
    pub fn level_range(&self) -> LevelRange {
        match self {
            Codec::Snappy | Codec::Lz4 => LevelRange {
                first_level: 1,
                last_level: 1,
            },
            Codec::Zstd => LevelRange {
                first_level: 1,
                last_level: 22,
            },
            Codec::Gzip => LevelRange {
                first_level: 1,
                last_level: 9,
            },
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

    /// Compress `src` at `level`.
    ///
    /// # Errors
    /// Returns [`Error::Level`] when `level` is outside [`Codec::level_range`],
    /// or [`Error::Codec`] when the backend fails.
    pub fn compress(&self, level: u8, src: &[u8]) -> Result<Vec<u8>, Error> {
        check_level(self, level)?;
        match self {
            Codec::Snappy => crate::third_party::snappy::compress(src).map_err(codec_error),
            Codec::Zstd => crate::third_party::zstd::compress(src, level).map_err(codec_error),
            Codec::Lz4 => crate::third_party::lz4::compress(src).map_err(codec_error),
            Codec::Gzip => crate::third_party::flate2::compress(src, level).map_err(codec_error),
        }
    }

    /// Decompress `src` into at most `out_cap` bytes.
    ///
    /// # Errors
    /// Returns [`Error::Level`] when `level` is outside [`Codec::level_range`]
    /// or [`Error::Codec`] when the backend fails.
    pub fn decompress(&self, level: u8, src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
        check_level(self, level)?;
        match self {
            Codec::Snappy => {
                crate::third_party::snappy::decompress(src, out_cap).map_err(codec_error)
            }
            Codec::Zstd => crate::third_party::zstd::decompress(src, out_cap).map_err(codec_error),
            Codec::Lz4 => crate::third_party::lz4::decompress(src, out_cap).map_err(codec_error),
            Codec::Gzip => {
                crate::third_party::flate2::decompress(src, out_cap).map_err(codec_error)
            }
        }
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

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Compress `src` into one frame of `codec` at `level`: the standalone
/// container, with its own header and end mark, that a frame decoder
/// (`zstdcat`, `lz4cat`) reads — not the bare block [`Codec::compress`]
/// produces. Only a codec with a frame format has one.
///
/// # Errors
/// Returns [`Error::Level`] when `level` is outside [`Codec::level_range`],
/// [`Error::Frame`] for a codec without a frame format, or [`Error::Codec`]
/// when the backend fails.
pub fn compress_frame(codec: Codec, level: u8, src: &[u8]) -> Result<Vec<u8>, Error> {
    check_level(&codec, level)?;
    match codec {
        Codec::Zstd => crate::third_party::zstd::compress_frame(src, level).map_err(codec_error),
        Codec::Lz4 => crate::third_party::lz4::compress_frame(src).map_err(codec_error),
        Codec::Snappy | Codec::Gzip => Err(Error::Frame {
            codec: codec.name(),
        }),
    }
}

/// Decompress one frame of `codec`, whatever length it decodes to.
///
/// # Errors
/// Returns [`Error::Frame`] for a codec without a frame format, or
/// [`Error::Codec`] when the backend fails.
pub fn decompress_frame(codec: Codec, src: &[u8]) -> Result<Vec<u8>, Error> {
    match codec {
        Codec::Zstd => crate::third_party::zstd::decompress_frame(src).map_err(codec_error),
        Codec::Lz4 => crate::third_party::lz4::decompress_frame(src).map_err(codec_error),
        Codec::Snappy | Codec::Gzip => Err(Error::Frame {
            codec: codec.name(),
        }),
    }
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
            return Err(Error::RoundTrip);
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
    use std::io::Write;

    const LINE: &str = "the quick brown fox jumps over the lazy dog 0123456789 ";
    let mut out = Vec::with_capacity(1024 * 1024);
    let mut number = 0u32;
    while out.len() < 1024 * 1024 {
        writeln!(out, "{LINE}{number:08}").expect("writing to a Vec cannot fail");
        number += 1;
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

    /// A frame is the standalone container: it starts with its codec's magic
    /// number, and it decodes without a declared capacity.
    #[test]
    fn frames_round_trip_through_their_magic() {
        let src = one_mb_sample();
        let zstd = [0x28, 0xb5, 0x2f, 0xfd];
        let lz4 = [0x04, 0x22, 0x4d, 0x18];
        for (codec, magic) in [(Codec::Zstd, &zstd[..]), (Codec::Lz4, &lz4[..])] {
            let frame = compress_frame(codec, 1, &src).unwrap();
            assert!(frame.starts_with(magic), "{codec} wrote no frame");
            assert!(frame.len() < src.len(), "{codec} frame is no smaller");
            assert_eq!(decompress_frame(codec, &frame).unwrap(), src);
        }
        assert!(matches!(
            compress_frame(Codec::Snappy, 1, b"x"),
            Err(Error::Frame { .. })
        ));
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
        assert!(matches!(
            Codec::Snappy.compress(2, b"x"),
            Err(Error::Level { .. })
        ));
    }

    #[test]
    fn snappy_round_trip_one_mb() {
        let src = one_mb_sample();
        let c = Codec::Snappy.compress(1, &src).unwrap();
        let d = Codec::Snappy.decompress(1, &c, src.len()).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn snappy_compressible_input_shrinks() {
        let src = b"the quick brown fox jumps over the lazy dog ".repeat(16 * 1024);
        let c = Codec::Snappy.compress(1, &src).unwrap();
        assert!(c.len() < src.len());
    }

    #[test]
    fn zstd_round_trip_one_mb() {
        let src = one_mb_sample();
        let c = Codec::Zstd.compress(3, &src).unwrap();
        let d = Codec::Zstd.decompress(3, &c, src.len()).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn zstd_higher_level_compresses_smaller() {
        let src = one_mb_sample();
        let c1 = Codec::Zstd.compress(1, &src).unwrap();
        let c3 = Codec::Zstd.compress(3, &src).unwrap();
        assert!(c3.len() <= c1.len(), "zstd-3 should be <= zstd-1 size");
    }

    #[test]
    fn lz4_round_trip_one_mb() {
        let src = one_mb_sample();
        let c = Codec::Lz4.compress(1, &src).unwrap();
        let d = Codec::Lz4.decompress(1, &c, src.len()).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn lz4_compressible_input_shrinks() {
        let src = b"the quick brown fox jumps over the lazy dog ".repeat(16 * 1024);
        let c = Codec::Lz4.compress(1, &src).unwrap();
        assert!(c.len() < src.len());
    }

    #[test]
    fn gzip_round_trip_one_mb() {
        let src = one_mb_sample();
        let c = Codec::Gzip.compress(6, &src).unwrap();
        let d = Codec::Gzip.decompress(6, &c, src.len()).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn gzip_compressible_input_shrinks() {
        let src = one_mb_sample();
        let c = Codec::Gzip.compress(6, &src).unwrap();
        assert!(c.len() < src.len());
    }
}
