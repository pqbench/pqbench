//! `control`: a rewrite measured against a control written by the same writer
//! at the same codec and level, preserving the sample's row-group boundary.

use parquet::basic::{Compression, Encoding, GzipLevel, ZstdLevel};

use crate::v1;

use super::price::{price, Settings, Values};
use super::{ColumnSample, Error};

/// A rewrite's measured advantage over its control, as a fraction of the
/// control's bytes. `ratio_low` is the worst compression level.
pub(crate) struct Measurement {
    pub(crate) ratio_low: f64,
    pub(crate) ratio_high: f64,
    pub(crate) levels: String,
}

/// The codec family both arms use, or `None` when this writer cannot reproduce
/// the source's codec.
pub(super) fn family(codec: Option<&str>) -> Option<&'static str> {
    let rendered = codec?.to_lowercase();
    Some(match rendered.split('(').next()?.trim() {
        "uncompressed" | "none" => "uncompressed",
        "snappy" => "snappy",
        "gzip" => "gzip",
        "lz4" | "lz4_raw" => "lz4",
        "zstd" => "zstd",
        _ => return None,
    })
}

/// The compressions both arms test. Parquet footers do not record compression
/// levels, so zstd and gzip test a low-to-default bracket rather than trusting
/// a placeholder level.
fn compressions(codec: Option<&str>) -> Option<(Vec<Compression>, &'static str)> {
    Some(match family(codec)? {
        "uncompressed" => (vec![Compression::UNCOMPRESSED], "uncompressed"),
        "snappy" => (vec![Compression::SNAPPY], "snappy"),
        "lz4" => (vec![Compression::LZ4_RAW], "lz4"),
        "zstd" => (
            vec![
                Compression::ZSTD(ZstdLevel::try_new(1).ok()?),
                Compression::ZSTD(ZstdLevel::try_new(3).ok()?),
            ],
            "zstd levels 1-3",
        ),
        "gzip" => (
            vec![
                Compression::GZIP(GzipLevel::try_new(1).ok()?),
                Compression::GZIP(GzipLevel::try_new(6).ok()?),
            ],
            "gzip levels 1-6",
        ),
        _ => return None,
    })
}

/// Whether a chunk records `encoding` anywhere in its encoding set.
///
/// A chunk's set always lists `PLAIN` for its dictionary page, so this answers
/// "did the writer use this encoding", not "do the data pages use it".
pub(super) fn records(mass: &v1::Bytemass, encoding: Encoding) -> bool {
    mass.encodings.contains(&encoding.to_string())
}

/// The source byte-array column's own encoding, when the footer records one.
pub(super) fn source_encoding(mass: &v1::Bytemass) -> Option<Encoding> {
    if records(mass, Encoding::DELTA_LENGTH_BYTE_ARRAY) {
        Some(Encoding::DELTA_LENGTH_BYTE_ARRAY)
    } else if records(mass, Encoding::DELTA_BYTE_ARRAY) {
        Some(Encoding::DELTA_BYTE_ARRAY)
    } else {
        None
    }
}

/// One arm of a controlled pair: the values to encode and how to encode them.
pub(super) struct Arm<'a> {
    pub(super) values: Values<'a>,
    pub(super) settings: Settings,
}

/// Encode `baseline` with the source's own settings and `candidate` with the
/// proposed change, at the same codec and levels, and return the fraction
/// saved. `None` when the writer cannot reproduce the source codec or the
/// control encodes to nothing.
pub(super) fn compare(
    sample: &ColumnSample<'_>,
    baseline: &Arm<'_>,
    candidate: &Arm<'_>,
) -> Result<Option<Measurement>, Error> {
    measure_arms(sample, |compression| {
        let baseline_bytes = price(baseline.values, compression, baseline.settings)?;
        let candidate_bytes = price(candidate.values, compression, candidate.settings)?;
        Ok(Some((baseline_bytes, candidate_bytes)))
    })
}

/// Fold each compression level's baseline and candidate bytes into the
/// measurement: the lowest and highest fraction the candidate saves. `arms`
/// returns `None` for a level that cannot price the sample.
pub(super) fn measure_arms(
    sample: &ColumnSample<'_>,
    mut arms: impl FnMut(Compression) -> Result<Option<(u64, u64)>, Error>,
) -> Result<Option<Measurement>, Error> {
    let Some((compressions, levels)) = compressions(sample.mass.codec.as_deref()) else {
        return Ok(None);
    };
    let mut ratio_low = f64::INFINITY;
    let mut ratio_high = f64::NEG_INFINITY;
    for compression in compressions {
        let Some((baseline_bytes, candidate_bytes)) = arms(compression)? else {
            continue;
        };
        if baseline_bytes == 0 {
            return Ok(None);
        }
        let ratio = (baseline_bytes as f64 - candidate_bytes as f64) / baseline_bytes as f64;
        ratio_low = ratio_low.min(ratio);
        ratio_high = ratio_high.max(ratio);
    }
    if !ratio_low.is_finite() || !ratio_high.is_finite() {
        return Ok(None);
    }
    Ok(Some(Measurement {
        ratio_low,
        ratio_high,
        levels: levels.to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use parquet::basic::Encoding;

    use super::{compressions, family, records, source_encoding};
    use crate::v1::Bytemass;

    #[test]
    fn codecs_and_levels_bracket_unknown_footer_levels() {
        assert_eq!(family(Some("ZSTD")), Some("zstd"));
        assert_eq!(family(Some("ZSTD(ZstdLevel(1))")), Some("zstd"));
        assert_eq!(compressions(Some("ZSTD")).unwrap().0.len(), 2);
        assert_eq!(compressions(Some("GZIP")).unwrap().0.len(), 2);
        assert_eq!(compressions(Some("GZIP(GzipLevel(9))")).unwrap().0.len(), 2);
        assert_eq!(compressions(Some("SNAPPY")).unwrap().0.len(), 1);
        assert!(compressions(Some("BROTLI")).is_none());
        assert!(compressions(None).is_none());
    }

    #[test]
    fn a_recorded_encoding_is_the_control() {
        let mut mass = Bytemass::default();
        assert_eq!(source_encoding(&mass), None);
        assert!(!records(&mass, Encoding::DELTA_BINARY_PACKED));
        mass.encodings = vec!["PLAIN".into(), "DELTA_LENGTH_BYTE_ARRAY".into()];
        assert_eq!(
            source_encoding(&mass),
            Some(Encoding::DELTA_LENGTH_BYTE_ARRAY)
        );
        mass.encodings = vec!["DELTA_BYTE_ARRAY".into()];
        assert_eq!(source_encoding(&mass), Some(Encoding::DELTA_BYTE_ARRAY));
        mass.encodings = vec!["PLAIN".into(), "DELTA_BINARY_PACKED".into()];
        assert!(records(&mass, Encoding::DELTA_BINARY_PACKED));
        assert_eq!(source_encoding(&mass), None);
    }
}
