//! `base64-text`: standard or URL-safe Base64 text priced as its decoded
//! bytes.
//!
//! Base64 costs four characters for every three bytes, so a binary payload
//! stored as Base64 text is a third larger than the bytes it carries, and a
//! codec cannot recover that. The rule reads the text itself: a column is a
//! candidate only when nearly every value is a canonical Base64 spelling of
//! binary bytes, so canonical UUIDs, hex digests, and ordinary words never
//! reach a rewrite. The control then writes the same bytes as a `BYTE_ARRAY`
//! under the file's own codec, and the difference is what the rewrite saves.

use base64::engine::{general_purpose, Engine as _, GeneralPurpose};
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::{Settings, Values};
use super::writer::Writer;
use super::{py_string, ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const BASE64_TEXT_RULE: &str = "base64-text";

/// The shortest Base64 text the rule reads: 24 characters carry 18 raw bytes,
/// the least a length prefix would not swamp.
const MINIMUM_LENGTH: usize = 24;

/// The fewest raw bytes a decoded value must carry.
const MINIMUM_BYTES: usize = 16;

/// The fewest sampled non-null values a column decision rests on.
const MINIMUM_VALUES: usize = 16;

/// The fraction of sampled non-null values that must decode to binary bytes.
const VALID_FRACTION: f64 = 0.99;

/// Above this fraction of printable bytes, a decoded payload is text that
/// happened to be spelled in Base64, not a binary payload: a column of English
/// (or JSON) text encoded in Base64 is a text column to rewrite on its own
/// terms, not a binary one.
const PRINTABLE_FRACTION: f64 = 0.75;

/// The finding this column earns when nearly every sampled non-null value is
/// Base64 text of binary bytes.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.byte_array() || sample.cells().is_empty() {
        return None;
    }
    let mut counted = 0usize;
    let mut decodable = 0usize;
    for bytes in sample.cells().iter().flatten() {
        counted += 1;
        if decode(bytes.data()).is_some() {
            decodable += 1;
        }
    }
    if counted < MINIMUM_VALUES {
        return None;
    }
    let fraction = decodable as f64 / counted as f64;
    if fraction < VALID_FRACTION {
        return None;
    }
    Some(Finding {
        message: format!(
            "Base64 text at {:.2} B/value ({:.0}% of values decode)",
            sample.text_bpv,
            fraction * 100.0
        ),
        remedy: remedy(sample.writer_kind(), sample.leaf),
        remedies: remedies(sample.leaf),
        measure,
    })
}

/// Price the decoded bytes as a `BYTE_ARRAY` against the source's Base64 text,
/// both written by the same writer at the file's codec and level.
///
/// The control needs every value: a column with a stray non-Base64 value is not
/// the same column rewritten, so it stays unmeasured rather than credited with
/// the decodable values' bytes.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let mut raw = Vec::with_capacity(sample.cells().len());
    for cell in sample.cells() {
        match cell {
            Some(bytes) => match decode(bytes.data()) {
                Some(decoded) => raw.push(Some(ByteArray::from(decoded))),
                None => return Ok(None),
            },
            None => raw.push(None),
        }
    }
    let baseline = Arm {
        values: sample
            .values()
            .ok_or_else(|| Error::Invalid("the sample's byte cells were not decoded".into()))?,
        settings: Settings {
            dictionary: sample.chunk_has_dictionary,
            dictionary_page_size_limit: sample.dictionary_page_size_limit(),
            data_page_size_limit: None,
            encoding: control::source_encoding(sample.mass),
            version: WriterVersion::PARQUET_1_0,
        },
    };
    let candidate = Arm {
        values: Values::Bytes(&raw),
        settings: Settings {
            dictionary: false,
            dictionary_page_size_limit: None,
            data_page_size_limit: None,
            encoding: None,
            version: WriterVersion::PARQUET_1_0,
        },
    };
    control::compare(sample, &baseline, &candidate)
}

/// The settings that decode one column's Base64 text to binary, one per engine.
///
/// Each engine's remedy keeps the column's key quoted: a dotted, colon-bearing,
/// or `@` name is not a bare Python or SQL identifier.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    let quoted = py_string(leaf);
    let sql = format!("`{leaf}`");
    vec![
        v1::Remedy::new(
            "spark",
            format!("CAST(unbase64({sql}) AS BINARY) before write"),
        ),
        v1::Remedy::new(
            "duckdb",
            format!("CAST(from_base64({sql}) AS BLOB) in the COPY ... SELECT"),
        ),
        v1::Remedy::new(
            "pyarrow",
            format!(
                "base64.b64decode the column, then \
                 column_types={{{quoted}: pa.binary()}}"
            ),
        ),
    ]
}

/// The setting that decodes one column's Base64 text, in the detected writer's
/// terms.
fn remedy(writer: Writer, leaf: &str) -> String {
    let remedies = remedies(leaf);
    let setting = |engine: &str| super::setting(&remedies, engine);
    let lead = "decode the Base64 text to binary";
    match writer {
        Writer::ParquetMr => format!("{lead}: {}", setting("spark")),
        Writer::Photon => format!(
            "{lead}: {} (Photon forwarding unverified)",
            setting("spark")
        ),
        Writer::ArrowCpp => format!("{lead}: {}", setting("pyarrow")),
        Writer::ArrowRs => format!("{lead}: decode it before the writer sees it"),
        Writer::Other => format!("{lead}: {}", setting("duckdb")),
    }
}

/// Decode one value's Base64, standard or URL-safe, when it is binary bytes.
///
/// The shape checks reject what merely *looks* like Base64: a canonical UUID
/// (its dashes are URL-safe Base64 characters), a hex digest (its digits are
/// Base64 characters too), and a word (all letters, no digit). The alphabet,
/// the padding, and the trailing bits must be canonical, so a text value that
/// happens to use only Base64 characters is not read as one.
fn decode(text: &[u8]) -> Option<Vec<u8>> {
    if text.len() < MINIMUM_LENGTH || !text.is_ascii() {
        return None;
    }
    if super::uuid::parse_uuid(text).is_some() || text.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    // A word has letters only: no digit, no `+`, `/`, `-`, `_`, and no padding.
    if text.iter().all(u8::is_ascii_alphabetic) {
        return None;
    }
    let raw = ENGINES.iter().find_map(|engine| engine.decode(text).ok())?;
    (raw.len() >= MINIMUM_BYTES && binary(&raw)).then_some(raw)
}

/// The engines that spell one binary payload: the standard and URL-safe
/// alphabets, padded or unpadded. Each refuses a spelling that is not
/// canonical (its padding in the wrong place, its unused trailing bits set),
/// so a value either alphabet reads decodes to the same bytes, and one no
/// engine reads is not Base64 of a payload.
const ENGINES: [&GeneralPurpose; 4] = [
    &general_purpose::STANDARD,
    &general_purpose::STANDARD_NO_PAD,
    &general_purpose::URL_SAFE,
    &general_purpose::URL_SAFE_NO_PAD,
];

/// Whether bytes are a binary payload rather than printable text.
fn binary(raw: &[u8]) -> bool {
    let printable = raw
        .iter()
        .filter(|byte| {
            matches!(**byte, b'\t' | b'\n' | b'\r') || byte.is_ascii_graphic() || **byte == b' '
        })
        .count();
    printable as f64 / raw.len().max(1) as f64 <= PRINTABLE_FRACTION
}

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;

    use super::{decode, detect, measure};
    use crate::lint::{cells, uuid_text, ColumnSample};
    use crate::v1;

    fn column() -> v1::Column {
        v1::Column {
            physical_data_type: Some("BYTE_ARRAY".into()),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                compressed_bytes: Some(400_000),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    /// Deterministic incompressible payload bytes of the requested size.
    fn payload(index: u64, len: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(len);
        let mut state = 0x9e37_79b9_u32.wrapping_add(index as u32);
        while bytes.len() < len {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            bytes.push((state >> 24) as u8);
        }
        bytes
    }

    #[test]
    fn canonical_base64_decodes_to_its_bytes() {
        let raw = payload(7, 64);
        let text = STANDARD.encode(&raw);
        assert_eq!(decode(text.as_bytes()).as_deref(), Some(raw.as_slice()));
    }

    /// A Base64 spelling that is not canonical (its unused trailing bits are
    /// set) is not a value this rule reads.
    #[test]
    fn non_canonical_base64_is_rejected() {
        const ALPHABET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        // 19 bytes leave four unused bits in the last character before the
        // padding; setting one of them keeps the same bytes but is not a
        // canonical spelling of them.
        let text = STANDARD.encode(payload(3, 19));
        assert!(decode(text.as_bytes()).is_some());
        let at = text.find('=').expect("padded text") - 1;
        let mut bytes = text.into_bytes();
        let value = ALPHABET
            .find(bytes[at] as char)
            .expect("an alphabet character");
        bytes[at] = ALPHABET.as_bytes()[(value + 1) % 64];
        let non_canonical = String::from_utf8(bytes).expect("ascii");
        assert!(decode(non_canonical.as_bytes()).is_none());
    }

    /// UUIDs, hex digests, and words use Base64 characters without being
    /// Base64 of binary bytes.
    #[test]
    fn shapes_that_only_look_like_base64_are_rejected() {
        for text in [
            uuid_text(0x0011_2233),
            "00112233445566778899aabbccddeeff00112233".to_string(),
            "abcdefghijklmnopqrstuvwxyz".to_string(),
            "thequickbrownfoxjumpsoverthelazydog".to_string(),
            "dGhlIHF1aWNrIGJyb3duIGZveCBqdW1wcyBvdmVyIHRoZSBsYXp5IGRvZw==".to_string(),
        ] {
            assert!(
                decode(text.as_bytes()).is_none(),
                "{text} is not Base64 of a binary payload"
            );
        }
    }

    /// A trailing `=` pads a three-byte group, so it only belongs on a
    /// four-character boundary. A stray one on a `4k+1` length is not a spelling
    /// of the same bytes, so no engine reads it as one.
    #[test]
    fn a_stray_trailing_pad_is_rejected() {
        let unpadded = STANDARD.encode(payload(3, 18));
        assert_eq!(unpadded.len() % 4, 0);
        assert!(decode(unpadded.as_bytes()).is_some());

        let stray = format!("{unpadded}=");
        assert_eq!(stray.len() % 4, 1);
        assert!(decode(stray.as_bytes()).is_none());
    }

    /// A Base64 column of binary payloads is a candidate and the control shows
    /// the decoded bytes are smaller.
    #[test]
    fn base64_binary_payloads_earn_binary_bytes() {
        let values: Vec<String> = (0..2_000)
            .map(|index| STANDARD.encode(payload(index, 96)))
            .collect();
        let column = column();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);

        let finding = detect(&sample).expect("Base64 of binary payloads is a candidate");
        assert!(finding.message.contains("B/value"));
        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            measurement.ratio_low > 0.15,
            "Base64 costs a third more than its bytes, got {}",
            measurement.ratio_low
        );
    }

    /// Text columns never earn the rule, however their values are spelled.
    #[test]
    fn text_columns_are_not_candidates() {
        let plain: Vec<String> = (0..2_000)
            .map(|value| format!("value-{value}-with-some-words"))
            .collect();
        let cells_plain = cells(&plain);
        assert!(detect(&ColumnSample::for_test(&column(), &cells_plain, false)).is_none());

        let uuids: Vec<String> = (0..2_000).map(uuid_text).collect();
        let cells_uuid = cells(&uuids);
        assert!(detect(&ColumnSample::for_test(&column(), &cells_uuid, false)).is_none());

        let hex: Vec<String> = (0..2_000)
            .map(|value| format!("{value:016x}{:016x}", value))
            .collect();
        let cells_hex = cells(&hex);
        assert!(detect(&ColumnSample::for_test(&column(), &cells_hex, false)).is_none());

        let short: Vec<String> = (0..2_000)
            .map(|value| format!("dGVsbG8t{value:04}"))
            .collect();
        let cells_short = cells(&short);
        assert!(detect(&ColumnSample::for_test(&column(), &cells_short, false)).is_none());
    }

    /// A column with a stray text value stays unmeasured rather than credited
    /// with the decodable values' bytes.
    #[test]
    fn a_mixed_column_is_not_measured() {
        let mut values: Vec<String> = (0..2_000)
            .map(|index| STANDARD.encode(payload(index, 96)))
            .collect();
        values[0] = "not-base64-text-at-all".to_string();
        let column = column();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);

        assert!(detect(&sample).is_some(), "99% decode");
        assert!(measure(&sample).expect("measure").is_none());
    }
}
