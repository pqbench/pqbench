//! `uuid-text`: canonical UUID text priced as 16-byte binary.

use parquet::data_type::ByteArray;

use super::control::{self, Measurement};
use super::{ColumnSample, Error, Finding};

/// The rule id a suggestion carries.
pub const UUID_TEXT_RULE: &str = "uuid-text";

/// The finding this column earns when every sampled non-null value is a
/// canonical UUID. The control prices the same values as 16-byte binary.
pub(super) fn detect(sample: &ColumnSample<'_>) -> Option<Finding> {
    let mut annotated = true;
    for bytes in sample.groups.iter().flatten().flatten() {
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
        measure,
    })
}

/// Price the text arm against the binary arm, both written by the same writer
/// at the file's codec and level.
fn measure(sample: &ColumnSample<'_>) -> Result<Option<Measurement>, Error> {
    let binary: Option<Vec<Vec<Option<ByteArray>>>> = sample
        .groups
        .iter()
        .map(|cells| {
            cells
                .iter()
                .map(|cell| match cell {
                    Some(bytes) => {
                        parse_uuid(bytes.data()).map(|uuid| Some(ByteArray::from(uuid.as_slice())))
                    }
                    None => Some(None),
                })
                .collect()
        })
        .collect();
    let Some(binary) = binary else {
        return Ok(None);
    };
    control::compare(sample, sample.groups, &binary, false, None)
}

/// Parse canonical 36-byte UUID text into its 16 big-endian bytes.
fn parse_uuid(bytes: &[u8]) -> Option<[u8; 16]> {
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
    use super::parse_uuid;

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
}
