//! GZIP codec (gzip format, RFC 1952, via flate2 + C zlib).

use std::io::{Read, Write};

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
        let mut enc =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(u32::from(level)));
        enc.write_all(src)
            .map_err(|e| Error::Codec(e.to_string()))?;
        enc.finish().map_err(|e| Error::Codec(e.to_string()))
    }

    fn decompress(&self, _level: u8, src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
        let mut out = Vec::with_capacity(out_cap);
        flate2::read::GzDecoder::new(src)
            .take(out_cap as u64)
            .read_to_end(&mut out)
            .map_err(|e| Error::Codec(e.to_string()))?;
        Ok(out)
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
