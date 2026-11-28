//! `page-size`: a chunk split into many tiny data pages, priced against the
//! same values written with larger pages at the file's own codec and encoding.
//!
//! A tiny page-size limit writes many small pages: every page pays its own
//! header and starts its codec's context afresh, so the chunk compresses worse
//! than the same values in fewer, larger pages. The control reproduces the
//! source's effective page size and the candidate writes the standard 1 MiB
//! limit; the difference is what a larger page size saves.

use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::decode::{chunk_pages, ChunkPage};
use super::price::Settings;
use super::writer::Writer;
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const PAGE_SIZE_RULE: &str = "page-size";

/// The standard page size a rewrite should use.
const CANDIDATE_PAGE_BYTES: usize = 1024 * 1024;

/// A data page this small is a tiny page: a writer's page-size limit is well
/// below the 1 MiB the Parquet ecosystem writes.
const TINY_PAGE_BYTES: usize = 64 * 1024;

/// The fewest data pages a page-size decision rests on. One or two pages are
/// the file's shape, not a limit that splits a chunk.
const MIN_DATA_PAGES: usize = 4;

/// Above this many rows per page, the pages are the writer's row limit (20,000
/// rows in parquet-rs), not a byte-size limit: a larger page size would change
/// nothing, so the rule must not claim it.
const ROW_LIMITED_ROWS_PER_PAGE: usize = 16_000;

/// What a chunk's data pages say about its writer's page-size limit.
struct PageSizes {
    page_count: usize,
    average_payload_bytes: usize,
    largest_payload_bytes: usize,
}

/// The finding this column earns when its chunk splits into many tiny data
/// pages.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    sample.values()?;
    let sizes = page_sizes(sample).ok().flatten()?;
    Some(Finding {
        message: format!(
            "{} data pages average {} bytes (largest {}) at {:.2} B/value",
            sizes.page_count,
            sizes.average_payload_bytes,
            sizes.largest_payload_bytes,
            sample.text_bpv
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf),
        remedies: remedies(sample.leaf),
        measure,
    })
}

/// Measure the same values with the source's effective page size and with the
/// standard 1 MiB page size, both at the file's codec, encoding, and dictionary
/// state.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let Some(values) = sample.values() else {
        return Ok(None);
    };
    let Some(sizes) = page_sizes(sample)? else {
        return Ok(None);
    };
    let source_page_size = sizes.largest_payload_bytes.max(1);
    if source_page_size >= CANDIDATE_PAGE_BYTES {
        return Ok(None);
    }
    let version = match values {
        super::price::Values::FixedLen(..) => WriterVersion::PARQUET_2_0,
        _ => WriterVersion::PARQUET_1_0,
    };
    let settings = Settings {
        dictionary: sample.chunk_has_dictionary,
        dictionary_page_size_limit: sample.dictionary_page_size_limit(),
        data_page_size_limit: Some(source_page_size),
        encoding: control::source_encoding(sample.mass),
        version,
    };
    let baseline = Arm { values, settings };
    let candidate = Arm {
        values,
        settings: Settings {
            data_page_size_limit: Some(CANDIDATE_PAGE_BYTES),
            ..settings
        },
    };
    control::compare(sample, &baseline, &candidate)
}

/// The chunk's data pages, when they show a tiny page-size limit.
fn page_sizes(sample: &ColumnSample<'_>) -> Result<Option<PageSizes>, Error> {
    let pages = chunk_pages(sample.path, sample.row_group, sample.column_index)?;
    let data: Vec<&ChunkPage> = pages.iter().filter(|page| page.data).collect();
    if data.len() < MIN_DATA_PAGES {
        return Ok(None);
    }
    let rows_per_page =
        usize::try_from(sample.coverage.row_count).unwrap_or(usize::MAX) / data.len();
    if rows_per_page >= ROW_LIMITED_ROWS_PER_PAGE {
        return Ok(None);
    }
    let total: usize = data.iter().map(|page| page.payload.len()).sum();
    let average_payload_bytes = total / data.len();
    if average_payload_bytes >= TINY_PAGE_BYTES {
        return Ok(None);
    }
    let largest_payload_bytes = data
        .iter()
        .map(|page| page.payload.len())
        .max()
        .unwrap_or_default();
    Ok(Some(PageSizes {
        page_count: data.len(),
        average_payload_bytes,
        largest_payload_bytes,
    }))
}

/// The setting that writes larger data pages, one per engine.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "spark",
            format!("spark.hadoop.parquet.page.size={CANDIDATE_PAGE_BYTES}"),
        ),
        v1::Remedy::new("pyarrow", format!("data_page_size={CANDIDATE_PAGE_BYTES}")),
        v1::Remedy::new(
            "parquet-rs",
            format!("set_data_page_size_limit({CANDIDATE_PAGE_BYTES})"),
        ),
        v1::Remedy::new(
            "delta",
            format!("spark.hadoop.parquet.page.size={CANDIDATE_PAGE_BYTES}"),
        ),
        v1::Remedy::new(
            "iceberg",
            format!("write.parquet.page-size-bytes={CANDIDATE_PAGE_BYTES}"),
        ),
        v1::Remedy::new(
            "duckdb",
            format!(
                "no data-page-size knob for {leaf}: DuckDB sizes pages itself; raise \
                 row_group_size to write larger groups"
            ),
        ),
    ]
}

/// The setting that writes larger data pages, in the detected writer's terms.
fn remedy(writer: Writer, leaf: &str) -> String {
    let remedies = remedies(leaf);
    let setting = |engine: &str| super::setting(&remedies, engine);
    let lead = "write larger data pages";
    match writer {
        Writer::ParquetMr => format!("{lead}: {}", setting("spark")),
        Writer::Photon => format!(
            "{lead}: {} (Photon forwarding unverified)",
            setting("delta")
        ),
        Writer::ArrowCpp => format!("{lead}: {}", setting("pyarrow")),
        Writer::ArrowRs => format!("{lead}: {}", setting("parquet-rs")),
        Writer::Other => format!("{lead}: {}", setting("iceberg")),
    }
}

#[cfg(test)]
mod tests {
    use super::{remedy, CANDIDATE_PAGE_BYTES};
    use crate::lint::writer::Writer;

    #[test]
    fn each_writer_gets_a_setting_it_has() {
        assert!(remedy(Writer::ParquetMr, "id").contains("parquet.page.size=1048576"));
        assert!(remedy(Writer::ArrowCpp, "id").contains("data_page_size=1048576"));
        assert!(remedy(Writer::ArrowRs, "id").contains("set_data_page_size_limit(1048576)"));
        assert!(remedy(Writer::Other, "id").contains("write.parquet.page-size-bytes=1048576"));
        assert!(remedy(Writer::Photon, "id").contains("unverified"));
        assert_eq!(CANDIDATE_PAGE_BYTES, 1024 * 1024);
    }
}
