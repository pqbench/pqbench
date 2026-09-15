//! Zstandard codec (libzstd 1.5.7 via the `zstd` crate).

use super::{CodecImpl, Error, LevelRange};

/// The Zstandard codec (libzstd 1.5.7 via the `zstd` crate).
pub struct Zstd;

impl CodecImpl for Zstd {
    fn name(&self) -> &'static str {
        "zstd"
    }

    fn level_range(&self) -> LevelRange {
        LevelRange {
            first_level: 1,
            last_level: 22,
        }
    }

    fn compress(&self, level: u8, src: &[u8]) -> Result<Vec<u8>, Error> {
        zstd::bulk::compress(src, level as i32).map_err(|e| Error::Codec(e.to_string()))
    }

    fn decompress(&self, _level: u8, src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
        zstd::bulk::decompress(src, out_cap).map_err(|e| Error::Codec(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_one_mb() {
        let src = super::super::one_mb_sample();
        let c = Zstd.compress(3, &src).unwrap();
        let d = Zstd.decompress(3, &c, src.len()).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn higher_level_compresses_smaller() {
        let src = super::super::one_mb_sample();
        let c1 = Zstd.compress(1, &src).unwrap();
        let c3 = Zstd.compress(3, &src).unwrap();
        assert!(c3.len() <= c1.len(), "zstd-3 should be <= zstd-1 size");
    }
}
