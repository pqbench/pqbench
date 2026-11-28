//! Raw benchmarking: load the file once and record raw per-pass sizes and
//! times for each codec×level. Nothing is reduced here — that's `analytics`.

use std::fs;
use std::path::Path;
use std::time::Duration;

use crate::codecs::{bench_buffer, BufferSamples, Codec, Error};

/// One codec×level raw measurement over the file bytes.
pub struct RawRow {
    pub codec: Codec,
    pub level: u8,
    pub compress_durations: Vec<Duration>,
    pub decompress_durations: Vec<Duration>,
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
}

impl RawRow {
    fn from_buffer(codec: Codec, level: u8, r: BufferSamples) -> RawRow {
        RawRow {
            codec,
            level,
            compress_durations: r.compress_durations,
            decompress_durations: r.decompress_durations,
            compressed_bytes: r.compressed_bytes,
            uncompressed_bytes: r.uncompressed_bytes,
        }
    }
}

/// Load the file once and bench every config, verifying round-trip per codec.
pub fn bench_file(path: &Path, configs: &[(Codec, u8)], passes: u32) -> Result<Vec<RawRow>, Error> {
    let data = fs::read(path)?;
    let mut rows = Vec::with_capacity(configs.len());
    for &(codec, level) in configs {
        let r = bench_buffer(codec, level, &data, passes)?;
        rows.push(RawRow::from_buffer(codec, level, r));
    }
    Ok(rows)
}
