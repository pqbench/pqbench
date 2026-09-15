//! LZ4 codec (raw block format via the `lz4` crate).

use super::{CodecImpl, Error, LevelRange};

/// The LZ4 codec (raw block format via the `lz4` crate).
pub struct Lz4;

impl CodecImpl for Lz4 {
    fn name(&self) -> &'static str {
        "lz4"
    }

    fn level_range(&self) -> LevelRange {
        LevelRange {
            first_level: 1,
            last_level: 1,
        }
    }

    fn compress(&self, _level: u8, src: &[u8]) -> Result<Vec<u8>, Error> {
        lz4::block::compress(src, None, false).map_err(|e| Error::Codec(e.to_string()))
    }

    fn decompress(&self, _level: u8, src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
        lz4::block::decompress(src, Some(i32::try_from(out_cap).unwrap_or(i32::MAX)))
            .map_err(|e| Error::Codec(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_one_mb() {
        let src = super::super::one_mb_sample();
        let c = Lz4.compress(1, &src).unwrap();
        let d = Lz4.decompress(1, &c, src.len()).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn compressible_input_shrinks() {
        let src = b"the quick brown fox jumps over the lazy dog ".repeat(16 * 1024);
        let c = Lz4.compress(1, &src).unwrap();
        assert!(c.len() < src.len());
    }
}
