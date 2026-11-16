//! `control`: a rewrite measured against a control written by the same writer
//! at the same codec and level.

use crate::third_party::parquet::api::{Value, WriteOptions};
use crate::v1;

use super::price::price_bytes;
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
fn family(codec: Option<&str>) -> Option<&'static str> {
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

/// The levels both arms run at. The footer's zstd level is a writer
/// placeholder, not the level the file used, so test both ends; gzip records
/// its level.
fn levels(codec: Option<&str>) -> Vec<Option<i32>> {
    match family(codec) {
        Some("zstd") => vec![Some(1), Some(3)],
        Some("gzip") => vec![Some(recorded_level(codec).unwrap_or(6))],
        _ => vec![None],
    }
}

fn recorded_level(codec: Option<&str>) -> Option<i32> {
    let digits: String = codec?.chars().filter(char::is_ascii_digit).collect();
    digits.parse().ok()
}

fn label(codec: Option<&str>, levels: &[Option<i32>]) -> String {
    match family(codec) {
        Some("zstd") => "zstd levels 1-3".to_string(),
        Some("gzip") => format!(
            "gzip level {}",
            levels.first().copied().flatten().unwrap_or(6)
        ),
        Some(other) => other.to_string(),
        None => "an unsupported codec".to_string(),
    }
}

/// The source column's own encoding, when the footer records one.
pub(super) fn source_encoding(mass: &v1::Bytemass) -> Option<&'static str> {
    let has = |name: &str| mass.encodings.iter().any(|encoding| encoding == name);
    if has("DELTA_LENGTH_BYTE_ARRAY") {
        Some("delta_length")
    } else if has("DELTA_BYTE_ARRAY") {
        Some("delta_byte_array")
    } else if has("DELTA_BINARY_PACKED") {
        Some("delta")
    } else if has("BYTE_STREAM_SPLIT") {
        Some("byte_stream_split")
    } else {
        None
    }
}

/// Encode `baseline_rows` with the source's own settings and `candidate_rows`
/// with the proposed change, at the same codec and levels, and return the
/// fraction saved. `None` when the writer cannot reproduce the source codec or
/// the control encodes to nothing.
pub(super) fn compare(
    sample: &ColumnSample<'_>,
    baseline_rows: &[Vec<Value>],
    candidate_rows: &[Vec<Value>],
    dictionary: bool,
    encoding: Option<&'static str>,
) -> Result<Option<Measurement>, Error> {
    let codec = sample.mass.codec.as_deref();
    let Some(family) = family(codec) else {
        return Ok(None);
    };
    let levels = levels(codec);
    let source_encoding = source_encoding(sample.mass);
    let mut ratio_low = f64::INFINITY;
    let mut ratio_high = f64::NEG_INFINITY;
    for level in &levels {
        let baseline = options(family, *level, sample.mass.dictionary, source_encoding);
        let candidate = options(family, *level, dictionary, encoding);
        let baseline_bytes = price_bytes(baseline_rows, &baseline)?;
        if baseline_bytes == 0 {
            return Ok(None);
        }
        let candidate_bytes = price_bytes(candidate_rows, &candidate)?;
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
        levels: label(codec, &levels),
    }))
}

fn options(
    family: &str,
    level: Option<i32>,
    dictionary: bool,
    encoding: Option<&str>,
) -> WriteOptions {
    WriteOptions {
        compression: family.to_string(),
        level,
        dictionary,
        dictionary_bytes: None,
        encoding: encoding.map(ToString::to_string),
        page_size: None,
        sorting_columns: Vec::new(),
        row_group_size: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{family, levels, source_encoding};
    use crate::v1::Bytemass;

    #[test]
    fn codecs_and_levels_follow_the_footer() {
        assert_eq!(family(Some("ZSTD(ZstdLevel(1))")), Some("zstd"));
        assert_eq!(levels(Some("ZSTD(ZstdLevel(1))")), vec![Some(1), Some(3)]);
        assert_eq!(family(Some("SNAPPY")), Some("snappy"));
        assert_eq!(levels(Some("SNAPPY")), vec![None]);
        assert_eq!(levels(Some("GZIP(GzipLevel(9))")), vec![Some(9)]);
        assert_eq!(family(Some("BROTLI(BrotliLevel(1))")), None);
        assert_eq!(family(None), None);
    }

    #[test]
    fn a_recorded_encoding_is_the_control() {
        let mut mass = Bytemass::default();
        assert_eq!(source_encoding(&mass), None);
        mass.encodings = vec!["PLAIN".into(), "DELTA_LENGTH_BYTE_ARRAY".into()];
        assert_eq!(source_encoding(&mass), Some("delta_length"));
        mass.encodings = vec!["DELTA_BINARY_PACKED".into()];
        assert_eq!(source_encoding(&mass), Some("delta"));
    }
}
