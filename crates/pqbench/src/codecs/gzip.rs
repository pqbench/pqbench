//! GZIP codec (gzip format, RFC 1952, via flate2 + C zlib).

use super::{CodecImpl, Error, LevelRange};

/// The gzip codec (gzip format, RFC 1952, via flate2 + C zlib).
pub struct Gzip;

impl CodecImpl for Gzip {
    fn name(&self) -> &'static str {
        "gzip"
    }

    fn level_range(&self) -> LevelRange {
        LevelRange {
            first_level: 1,
            last_level: 9,
        }
    }

    fn compress(&self, level: u8, src: &[u8]) -> Result<Vec<u8>, Error> {
        crate::third_party::flate2::compress(src, level).map_err(|e| Error::Codec(e.to_string()))
    }

    fn decompress(&self, _level: u8, src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
        crate::third_party::flate2::decompress(src, out_cap)
            .map_err(|e| Error::Codec(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_one_mb() {
        let src = super::super::one_mb_sample();
        let c = Gzip.compress(6, &src).unwrap();
        let d = Gzip.decompress(6, &c, src.len()).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn compressible_input_shrinks() {
        let src = super::super::one_mb_sample();
        let c = Gzip.compress(6, &src).unwrap();
        assert!(c.len() < src.len());
    }
}
