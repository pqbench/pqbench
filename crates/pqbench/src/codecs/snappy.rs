//! Snappy codec (C snappy 1.2.2 via `snappy_src`).

use super::{CodecImpl, Error, LevelRange};

/// The Snappy codec (C snappy 1.2.2 via `snappy_src`).
pub struct Snappy;

impl CodecImpl for Snappy {
    fn name(&self) -> &'static str {
        "snappy"
    }

    fn level_range(&self) -> LevelRange {
        LevelRange {
            first_level: 1,
            last_level: 1,
        }
    }

    fn compress(&self, _level: u8, src: &[u8]) -> Result<Vec<u8>, Error> {
        let max_len = unsafe { snappy_src::snappy_max_compressed_length(src.len()) };
        let mut dst = Vec::with_capacity(max_len);
        let mut len = max_len;
        let status = unsafe {
            snappy_src::snappy_compress(
                src.as_ptr().cast(),
                src.len(),
                dst.spare_capacity_mut().as_mut_ptr().cast(),
                &mut len,
            )
        };
        if status != snappy_src::snappy_status_SNAPPY_OK {
            return Err(Error::Codec("snappy compress failed".to_string()));
        }
        unsafe { dst.set_len(len) };
        Ok(dst)
    }

    fn decompress(&self, _level: u8, src: &[u8], out_cap: usize) -> Result<Vec<u8>, Error> {
        let mut dst = Vec::with_capacity(out_cap);
        let mut len = out_cap;
        let status = unsafe {
            snappy_src::snappy_uncompress(
                src.as_ptr().cast(),
                src.len(),
                dst.spare_capacity_mut().as_mut_ptr().cast(),
                &mut len,
            )
        };
        if status != snappy_src::snappy_status_SNAPPY_OK {
            return Err(Error::Codec("snappy decompress failed".to_string()));
        }
        unsafe { dst.set_len(len) };
        Ok(dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_one_mb() {
        let src = super::super::one_mb_sample();
        let c = Snappy.compress(1, &src).unwrap();
        let d = Snappy.decompress(1, &c, src.len()).unwrap();
        assert_eq!(d, src);
    }

    #[test]
    fn compressible_input_shrinks() {
        let src = b"the quick brown fox jumps over the lazy dog ".repeat(16 * 1024);
        let c = Snappy.compress(1, &src).unwrap();
        assert!(c.len() < src.len());
    }

    #[test]
    fn level_is_ignored() {
        let src = b"hello, world! ".repeat(1000);
        let a = Snappy.compress(1, &src).unwrap();
        let b = Snappy.compress(99, &src).unwrap();
        assert_eq!(a, b);
    }
}
