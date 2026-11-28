//! `codec-zstd`: a column chunk zstd is expected to beat, priced by
//! recompressing the chunk's representative data page with no value decoding.
//!
//! Compression is per page, so a page's on-disk bytes are its own: the rule
//! picks the data page whose uncompressed size is closest to the chunk's
//! average data page and prices that page alone, which is what a codec decision
//! turns on without recompressing every page of every column.

use crate::codecs::Codec;
use crate::v1;

use super::control::{self, Measurement};
use super::decode::{chunk_pages, ChunkPage};
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const CODEC_ZSTD_RULE: &str = "codec-zstd";

/// The zstd levels to bracket: a footer does not record the level the file was
/// written at, so the candidate spans a low level and the common default.
const LEVELS: [u8; 2] = [1, 3];

/// The levels, as a verdict names them.
const LEVELS_TEXT: &str = "zstd levels 1-3";

/// The codec families zstd is expected to beat on a writer's own pages.
const WEAK_CODECS: [&str; 3] = ["uncompressed", "snappy", "gzip"];

/// The finding this column earns when its chunk is stored with a codec weaker
/// than zstd. Nothing is decoded: the footer's codec is the whole check.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    let family = control::family(sample.mass.codec.as_deref())?;
    if !WEAK_CODECS.contains(&family) {
        return None;
    }
    let leaf = sample.leaf;
    Some(Finding {
        message: format!("{family} at {:.2} B/value", sample.text_bpv),
        remedy: remedy(sample.writer_kind(), leaf),
        remedies: remedies(leaf),
        measure,
    })
}

/// Recompress the sample chunk's representative data page with zstd levels 1
/// and 3 and price the result against the bytes that page occupies on disk.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let pages = chunk_pages(sample.path, sample.row_group, sample.column_index)?;
    zstd_measurement(sample.chunk_compressed_bytes, &pages)
}

/// The saving zstd levels 1-3 show on the chunk's representative data page.
///
/// One page is priced when its own on-disk bytes are known: a page's compressed
/// bytes are its own, so its ratio stands for the chunk. Otherwise every page is
/// priced against the chunk's footer bytes (`compressed_size()`), which is exact
/// because they are the sum of every page's header and compressed payload. The
/// same whole-chunk pricing stands in when the chunk has a single data page,
/// whose bytes cannot stand apart from the dictionary page beside it.
fn zstd_measurement(chunk_bytes: u64, pages: &[ChunkPage]) -> Result<Option<Measurement>, Error> {
    let data_pages: Vec<&ChunkPage> = pages.iter().filter(|page| page.data).collect();
    if chunk_bytes == 0 || data_pages.is_empty() {
        return Ok(None);
    }
    if data_pages.len() > 1 {
        let page = representative_page(&data_pages);
        if let Some(compressed_bytes) = page.compressed_bytes.filter(|bytes| *bytes > 0) {
            return Ok(saving(compressed_bytes, &zstd_bytes(&page.payload)?));
        }
    }
    let mut candidates = [0_u64; LEVELS.len()];
    for page in pages {
        let candidate_bytes = zstd_bytes(&page.payload)?;
        for (at, bytes) in candidate_bytes.iter().enumerate() {
            candidates[at] += bytes;
        }
    }
    Ok(saving(chunk_bytes, &candidates))
}

/// The data page whose uncompressed payload is closest to the chunk's average
/// data page, breaking ties by the lowest index.
fn representative_page<'a>(pages: &[&'a ChunkPage]) -> &'a ChunkPage {
    let total_bytes: usize = pages.iter().map(|page| page.payload.len()).sum();
    let average_bytes = total_bytes as f64 / pages.len() as f64;
    let mut representative = pages[0];
    let mut closest = f64::INFINITY;
    for &page in pages {
        let distance = (page.payload.len() as f64 - average_bytes).abs();
        if distance < closest {
            representative = page;
            closest = distance;
        }
    }
    representative
}

/// The bytes zstd needs for `payload` at each level, in `LEVELS` order.
fn zstd_bytes(payload: &[u8]) -> Result<[u64; LEVELS.len()], Error> {
    let mut bytes = [0_u64; LEVELS.len()];
    for (at, level) in LEVELS.iter().enumerate() {
        bytes[at] = Codec::Zstd.compress(*level, payload)?.len() as u64;
    }
    Ok(bytes)
}

/// The fraction of `compressed_bytes` the candidates save: the largest candidate
/// is the worst level, the smallest the best.
fn saving(compressed_bytes: u64, candidates: &[u64; LEVELS.len()]) -> Option<Measurement> {
    if compressed_bytes == 0 {
        return None;
    }
    let largest = candidates.iter().copied().max().unwrap_or(compressed_bytes);
    let smallest = candidates.iter().copied().min().unwrap_or(compressed_bytes);
    let baseline = compressed_bytes as f64;
    Some(Measurement {
        ratio_low: (baseline - largest as f64) / baseline,
        ratio_high: (baseline - smallest as f64) / baseline,
        levels: LEVELS_TEXT.to_string(),
    })
}

/// The settings that write one column with zstd, one per engine. Each is the
/// engine's per-column form, so one column's codec and level change without
/// rewriting the table's other columns.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "spark",
            format!(
                "spark.hadoop.parquet.compression#{leaf}=ZSTD with \
                 spark.hadoop.parquet.compression.level#{leaf}=3, or \
                 spark.sql.parquet.compression.codec=zstd for the table"
            ),
        ),
        v1::Remedy::new(
            "pyarrow",
            format!("compression={{{leaf}: 'zstd'}}, compression_level={{{leaf}: 3}}"),
        ),
        v1::Remedy::new(
            "parquet-rs",
            format!("set_column_compression({leaf:?}, Compression::ZSTD(ZstdLevel::default()))"),
        ),
        v1::Remedy::new("delta", "'delta.parquet.compression.codec'='zstd'"),
        v1::Remedy::new("iceberg", "write.parquet.compression-codec=zstd"),
    ]
}

/// The setting that writes one column with zstd, in the detected writer's
/// terms.
fn remedy(writer: Writer, leaf: &str) -> String {
    let remedies = remedies(leaf);
    let setting = |engine: &str| super::setting(&remedies, engine);
    match writer {
        Writer::ParquetMr => setting("spark"),
        Writer::Photon => format!(
            "ALTER TABLE <table> SET TBLPROPERTIES ({}) (Photon forwarding unverified)",
            setting("delta")
        ),
        Writer::ArrowCpp => setting("pyarrow"),
        Writer::ArrowRs => setting("parquet-rs"),
        Writer::Other => format!("write the column with zstd ({})", setting("iceberg")),
    }
}

#[cfg(test)]
mod tests {
    use super::{detect, remedy, zstd_measurement};
    use crate::lint::decode::ChunkPage;
    use crate::lint::writer::Writer;
    use crate::lint::ColumnSample;
    use crate::v1;

    fn column(codec: &str) -> v1::Column {
        v1::Column {
            mass: Some(v1::Bytemass {
                codec: Some(codec.into()),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    /// A data page with the bytes it occupies on disk, as an offset index would
    /// record them.
    fn page(payload: Vec<u8>, compressed_bytes: Option<u64>) -> ChunkPage {
        ChunkPage {
            payload,
            compressed_bytes,
            data: true,
        }
    }

    /// Highly compressible bytes of the requested size.
    fn repeated(len: usize) -> Vec<u8> {
        b"the same repeated value,"
            .iter()
            .copied()
            .cycle()
            .take(len)
            .collect()
    }

    /// Deterministic incompressible bytes of the requested size.
    fn noise(len: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(len);
        let mut state = 0x9e37_79b9_u32;
        while bytes.len() < len {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            bytes.push((state >> 24) as u8);
        }
        bytes
    }

    #[test]
    fn weak_codecs_are_candidates_and_strong_ones_are_not() {
        let cells = Vec::new();
        for codec in ["UNCOMPRESSED", "SNAPPY", "GZIP", "GZIP(GzipLevel(9))"] {
            assert!(
                detect(&ColumnSample::for_test(&column(codec), &cells, false)).is_some(),
                "{codec}"
            );
        }
        for codec in ["ZSTD", "ZSTD(ZstdLevel(1))", "LZ4_RAW", "BROTLI"] {
            assert!(
                detect(&ColumnSample::for_test(&column(codec), &cells, false)).is_none(),
                "{codec}"
            );
        }
    }

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(
            remedy(Writer::ParquetMr, "id").contains("spark.hadoop.parquet.compression#id=ZSTD")
        );
        assert!(
            remedy(Writer::ParquetMr, "id").contains("spark.hadoop.parquet.compression.level#id=3")
        );
        assert!(remedy(Writer::ParquetMr, "id").contains("spark.sql.parquet.compression.codec"));
        assert!(remedy(Writer::Photon, "id").contains("'delta.parquet.compression.codec'='zstd'"));
        assert!(remedy(Writer::Photon, "id").contains("unverified"));
        assert_eq!(
            remedy(Writer::ArrowCpp, "id"),
            "compression={id: 'zstd'}, compression_level={id: 3}"
        );
        assert!(remedy(Writer::ArrowRs, "id").contains("set_column_compression"));
        assert!(remedy(Writer::Other, "id").contains("write.parquet.compression-codec=zstd"));
    }

    /// A chunk's single data page is priced against its whole chunk: repetitive
    /// pages compress well below the bytes a weaker codec left on disk.
    #[test]
    fn zstd_beats_a_weak_codec_on_repetitive_pages() {
        let payload = repeated(200 * 1024);
        let stored = (payload.len() / 2) as u64;

        let measurement = zstd_measurement(stored, &[page(payload, None)])
            .expect("measure")
            .expect("zstd prices the page");
        assert!(
            measurement.ratio_low > 0.5,
            "zstd must more than half the stored bytes, got {}",
            measurement.ratio_low
        );
        assert!(measurement.ratio_low <= measurement.ratio_high);
    }

    /// A chunk without an offset index prices every page against its footer
    /// bytes, which are the sum of the pages.
    #[test]
    fn a_chunk_without_an_offset_index_prices_every_page() {
        let stored = 200 * 1024;
        let measurement = zstd_measurement(
            stored,
            &[
                page(repeated(100 * 1024), None),
                page(repeated(100 * 1024), None),
            ],
        )
        .expect("measure")
        .expect("zstd prices the pages");
        assert!(
            measurement.ratio_low > 0.5,
            "both pages must be priced, got {}",
            measurement.ratio_low
        );
    }

    /// Only the representative data page is priced when each page records its
    /// own on-disk bytes: the page closest to the chunk's average data page is
    /// the 32 KiB one, so its bytes alone decide the rule.
    #[test]
    fn the_representative_data_page_is_the_one_priced() {
        let chunk_bytes = 4 * 1024 + 32 * 1024 + 64 * 1024;
        let compressible = zstd_measurement(
            chunk_bytes,
            &[
                page(noise(4 * 1024), Some(4 * 1024)),
                page(repeated(32 * 1024), Some(32 * 1024)),
                page(noise(64 * 1024), Some(64 * 1024)),
            ],
        )
        .expect("measure")
        .expect("zstd prices the page");
        assert!(
            compressible.ratio_low > 0.5,
            "the compressive page closest to the average must be the one priced, got {}",
            compressible.ratio_low
        );

        let incompressible = zstd_measurement(
            chunk_bytes,
            &[
                page(repeated(4 * 1024), Some(4 * 1024)),
                page(noise(32 * 1024), Some(32 * 1024)),
                page(repeated(64 * 1024), Some(64 * 1024)),
            ],
        )
        .expect("measure")
        .expect("zstd prices the page");
        assert!(
            incompressible.ratio_high < 0.02,
            "the incompressible page closest to the average must be the one priced, got {}",
            incompressible.ratio_high
        );
    }

    /// Equally sized data pages break the tie by the lowest index: the first
    /// page repeats, so the rule finds the saving the later noise pages hide.
    #[test]
    fn equal_data_pages_price_the_first() {
        let measurement = zstd_measurement(
            3 * 4096,
            &[
                page(repeated(4096), Some(4096)),
                page(noise(4096), Some(4096)),
                page(noise(4096), Some(4096)),
            ],
        )
        .expect("measure")
        .expect("zstd prices the page");
        assert!(
            measurement.ratio_low > 0.5,
            "the first page must be the one priced, got {}",
            measurement.ratio_low
        );
    }

    /// Incompressible pages show no saving, so the rule would be rejected.
    #[test]
    fn zstd_does_not_beat_stored_bytes_on_noise() {
        let payload = noise(64 * 1024);
        let measurement = zstd_measurement(payload.len() as u64, &[page(payload, None)])
            .expect("measure")
            .expect("zstd prices the page");
        assert!(
            measurement.ratio_high < 0.02,
            "noise must not compress, got {}",
            measurement.ratio_high
        );
    }
}
