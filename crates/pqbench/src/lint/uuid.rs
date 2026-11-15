//! `uuid-text`: canonical UUID text priced as 16-byte binary.

use crate::third_party::parquet::api::Value;

use super::price::price_bytes;
use super::{ColumnSample, Error, Suggestion};

/// The rule id a suggestion carries.
pub const UUID_TEXT_RULE: &str = "uuid-text";

/// The suggestion this column earns when every sampled non-null value is a
/// canonical UUID and the same values as 16-byte binary price smaller.
pub(super) fn suggest(sample: &ColumnSample<'_>) -> Result<Option<Suggestion>, Error> {
    let uuids: Option<Vec<Option<[u8; 16]>>> = sample
        .cells
        .iter()
        .map(|cell| match *cell {
            Some(bytes) => parse_uuid(bytes).map(Some),
            None => Some(None),
        })
        .collect();
    let Some(uuids) = uuids else {
        return Ok(None);
    };
    let rows: Vec<Vec<Value>> = uuids
        .iter()
        .map(|uuid| {
            vec![match uuid {
                Some(bytes) => Value::Bytes(bytes.to_vec()),
                None => Value::Null,
            }]
        })
        .collect();
    let binary_bpv = price_bytes(&rows, &sample.writer)? as f64 / sample.non_null as f64;
    let Some(savings) = sample.saving(binary_bpv) else {
        return Ok(None);
    };
    let annotated = uuids
        .iter()
        .flatten()
        .all(|bytes| (1..=8).contains(&(bytes[6] >> 4)) && (8..=11).contains(&(bytes[8] >> 4)));
    Ok(Some(sample.suggestion(
        UUID_TEXT_RULE,
        format!(
            "UUID text at {:.2} B/value; a 16-byte binary sample prices at {binary_bpv:.2} B/value",
            sample.text_bpv
        ),
        if annotated {
            "store as BINARY(16) with the UUID logical type"
        } else {
            "store as BINARY(16)"
        },
        savings,
    )))
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
