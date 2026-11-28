//! `uuid-text`: canonical UUID text priced as 16-byte binary.

use parquet::data_type::ByteArray;
use parquet::file::properties::WriterVersion;

use crate::v1;

use super::control::{self, Arm, Measurement};
use super::price::{Settings, Values};
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const UUID_TEXT_RULE: &str = "uuid-text";

/// A UUID's binary width, the `FIXED_LEN_BYTE_ARRAY` length the candidate is
/// written as.
const UUID_BYTES: i32 = 16;

/// The finding this column earns when every sampled non-null value is a
/// canonical UUID. The control prices the same values as 16-byte binary.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    if !sample.byte_array() {
        return None;
    }
    let mut annotated = true;
    for bytes in sample.cells().iter().flatten() {
        let uuid = parse_uuid(bytes.data())?;
        if !((1..=8).contains(&(uuid[6] >> 4)) && (8..=11).contains(&(uuid[8] >> 4))) {
            annotated = false;
        }
    }
    Some(Finding {
        message: format!("UUID text at {:.2} B/value", sample.text_bpv),
        remedy: if annotated {
            "store as BINARY(16) with the UUID logical type".to_string()
        } else {
            "store as BINARY(16)".to_string()
        },
        remedies: remedies(sample.leaf),
        measure,
    })
}

/// The settings that store the values as a 16-byte UUID, one per engine that
/// has a UUID type.
fn remedies(leaf: &str) -> Vec<v1::Remedy> {
    vec![
        v1::Remedy::new(
            "pyarrow",
            format!(
                "column_types={{{}: pa.uuid()}} (or pa.binary(16))",
                super::py_string(leaf)
            ),
        ),
        v1::Remedy::new(
            "parquet-rs",
            format!("write {leaf} as LogicalType::Uuid over FIXED_LEN_BYTE_ARRAY(16)"),
        ),
        v1::Remedy::new(
            "iceberg",
            format!("ALTER TABLE <table> ALTER COLUMN {leaf} TYPE uuid"),
        ),
    ]
}

/// Price the text arm against the binary arm, both written by the same writer
/// at the file's codec and level.
///
/// The binary arm is a `FIXED_LEN_BYTE_ARRAY(16)` column: the values are always
/// 16 bytes, so a `BYTE_ARRAY` column would pay a 4-byte length prefix the
/// remedy's `BINARY(16)` type does not.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let mut binary = Vec::with_capacity(sample.cells().len());
    for cell in sample.cells() {
        match cell {
            Some(bytes) => match parse_uuid(bytes.data()) {
                Some(uuid) => binary.push(Some(ByteArray::from(uuid.to_vec()))),
                None => return Ok(None),
            },
            None => binary.push(None),
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
        values: Values::FixedLen(UUID_BYTES, &binary),
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

/// Parse canonical 36-byte UUID text into its 16 big-endian bytes.
pub(super) fn parse_uuid(bytes: &[u8]) -> Option<[u8; 16]> {
    if bytes.len() != 36 {
        return None;
    }
    let mut out = [0_u8; 16];
    let mut at = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if *byte != b'-' {
                return None;
            }
            continue;
        }
        let value = (*byte as char).to_digit(16)?;
        if at % 2 == 0 {
            out[at / 2] = (value as u8) << 4;
        } else {
            out[at / 2] |= value as u8;
        }
        at += 1;
    }
    (at == 32).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::{measure, parse_uuid};
    use crate::lint::{cells, uuid_text, ColumnSample};
    use crate::v1;

    fn column() -> v1::Column {
        v1::Column {
            physical_data_type: Some("BYTE_ARRAY".into()),
            mass: Some(v1::Bytemass {
                codec: Some("UNCOMPRESSED".into()),
                ..v1::Bytemass::default()
            }),
            ..v1::Column::default()
        }
    }

    /// The pyarrow remedy's column key is a Python string literal, so a dotted,
    /// colon-bearing, or `@` name is quoted like any other.
    #[test]
    fn the_pyarrow_remedy_quotes_exotic_column_keys() {
        for leaf in ["request.ext", "site.publisher:ext", "request.@timestamp"] {
            let setting = super::remedies(leaf)
                .into_iter()
                .find(|remedy| remedy.engine == "pyarrow")
                .expect("a pyarrow remedy")
                .setting;
            assert!(
                setting.contains(&format!("column_types={{'{leaf}': pa.uuid()}}")),
                "{setting}"
            );
        }
    }

    #[test]
    fn canonical_uuid_text_parses_to_big_endian_bytes() {
        let bytes = parse_uuid(b"00112233-4455-6677-8899-aabbccddeeff").expect("uuid");
        assert_eq!(
            bytes,
            [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]
        );
    }

    #[test]
    fn malformed_uuid_text_is_rejected() {
        assert!(parse_uuid(b"00112233445566778899aabbccddeeff00").is_none());
        assert!(parse_uuid(b"00112233-4455-6677-8899-aabbccddeefg").is_none());
        assert!(parse_uuid(b"00112233-4455-6677-8899-aabbccddeef").is_none());
    }

    /// 16 binary bytes per value, with no length prefix: a variable-length
    /// `BYTE_ARRAY` candidate would save only half of the 36-byte text, not
    /// three fifths.
    #[test]
    fn the_binary_arm_is_priced_as_fixed_length() {
        let column = column();
        let values: Vec<String> = (0..1_000).map(uuid_text).collect();
        let cells = cells(&values);
        let sample = ColumnSample::for_test(&column, &cells, false);

        let measurement = measure(&sample).expect("measure").expect("uncompressed");
        assert!(
            (0.55..0.65).contains(&measurement.ratio_low),
            "a FIXED_LEN_BYTE_ARRAY(16) candidate saves about 0.6, got {}",
            measurement.ratio_low
        );
    }
}
