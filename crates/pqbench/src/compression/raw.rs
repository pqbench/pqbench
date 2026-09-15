//! Raw benchmarking: for each codec×level, sweep every column chunk of an
//! already-parsed NONE-compressed file, recording per-chunk and per-page sizes
//! and raw per-pass times. Nothing is reduced here — that's `analytics`.

use std::time::Duration;

use crate::codecs::{bench_pages, Codec, Error, PageSamples};
use crate::parquet_helpers::ParquetFile;

/// One page's raw measurement for one codec×level.
pub struct PageResult {
    /// The uncompressed (encoded) payload size this page.
    pub uncompressed_bytes: usize,
    /// The size after this codec×level compressed the payload.
    pub compressed_bytes: usize,
    /// One timed compress pass per element, in pass order.
    pub compress_durations: Vec<Duration>,
    /// One timed decompress pass per element, in pass order.
    pub decompress_durations: Vec<Duration>,
}

/// One column chunk's raw measurements for one codec×level.
pub struct ChunkResult {
    /// Column path in schema form, e.g. `content` or `a.b`.
    pub column: String,
    /// Per-page measurements, in page order.
    pub pages: Vec<PageResult>,
    /// One timed full-chunk compress pass per element, in order.
    pub compress_durations: Vec<Duration>,
    /// One timed full-chunk decompress pass per element, in order.
    pub decompress_durations: Vec<Duration>,
}

/// One codec×level raw measurement over all chunks of a file.
pub struct RawRow {
    pub codec: Codec,
    pub level: u8,
    /// Per-column-chunk measurements, in file order.
    pub chunks: Vec<ChunkResult>,
}

/// Bench every config over an already-parsed file's page payloads.
///
/// The file must be NONE-compressed (see `parquet_helpers`); each page payload
/// is the encoded byte blob the codec actually compresses. Each chunk is swept
/// independently (per-column measurements), so chunk times are independent and
/// compose into a file measurement in the analytics layer.
pub fn bench_file(
    file: &ParquetFile,
    configs: &[(Codec, u8)],
    passes: u32,
) -> Result<Vec<RawRow>, Error> {
    let mut rows = Vec::with_capacity(configs.len());
    for &(codec, level) in configs {
        let mut chunks = Vec::with_capacity(file.chunks.len());
        for chunk in &file.chunks {
            let payloads: Vec<&[u8]> = chunk.pages.iter().map(|p| p.payload.as_slice()).collect();
            let PageSamples {
                page_compressed_sizes,
                compress_durations,
                decompress_durations,
                page_compress_durations,
                page_decompress_durations,
            } = bench_pages(codec, level, &payloads, passes)?;
            let pages = payloads
                .iter()
                .zip(page_compressed_sizes)
                .zip(page_compress_durations)
                .zip(page_decompress_durations)
                .map(
                    |(((payload, compressed_bytes), compress_durations), decompress_durations)| {
                        PageResult {
                            uncompressed_bytes: payload.len(),
                            compressed_bytes,
                            compress_durations,
                            decompress_durations,
                        }
                    },
                )
                .collect();
            chunks.push(ChunkResult {
                column: chunk.column.clone(),
                pages,
                compress_durations,
                decompress_durations,
            });
        }
        rows.push(RawRow {
            codec,
            level,
            chunks,
        });
    }
    Ok(rows)
}
