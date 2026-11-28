//! The JSON facade parses and writes what `serde_json` does.
//!
//! Which parser runs behind the facade is a build choice (`json-simd`), so this
//! is the contract a caller can hold it to: the documents the tools read parse
//! to the value `serde_json` parses them to, a document that is not JSON fails
//! either way, and writing is unchanged. Every case here is a shape the tools
//! actually read — a table file, an escaped path, a stats blob, a commit action,
//! a signed and a large number.

use pqbench::third_party::json;
use serde::Deserialize;
use serde_json::Value;

/// The documents the tools read, as bytes.
const DOCUMENTS: [&[u8]; 8] = [
    br#"{"name":"catalogs/local/schemas/local/tables/t/files/f.parquet","size_bytes":18446744073709551615}"#,
    br#"{"name":"part\u00e9.parquet","path":"part \u00e9.parquet"}"#,
    br#"{"stats":{"numRecords":3,"minValues":{"id":1,"note":null},"maxValues":{"id":3,"note":"z"},"nullCount":{"id":0,"note":2},"tightBounds":true}}"#,
    br#"{"add":{"path":"part=a/old%20file.parquet","size":796,"modificationTime":1760000000000,"dataChange":true}}"#,
    br#"{"timestamp":-1700000000000,"ratio":1.5,"exponent":1e300,"negative":-0.25}"#,
    br#"[{"a":1},{"b":[true,false,null]}]"#,
    br#""a bare name""#,
    b"{\n  \"name\": \"x\",\n  \"size_bytes\": 1\n}\n",
];

/// A commit's `add.stats` blob, as the delta loader types it.
const STATS: &[u8] = br#"{"numRecords":3,"minValues":{"id":1},"maxValues":{"id":3},"nullCount":{"id":0},"tightBounds":true}"#;

/// The documents that are not one JSON value.
const NOT_JSON: [&[u8]; 5] = [b"{", b"", b"{\"a\":}", b"{} {}", b"{\"a\": \"\xff\"}"];

#[test]
fn parsing_yields_the_value_serde_json_yields() {
    for document in DOCUMENTS {
        let reference: Value = serde_json::from_slice(document).expect("the reference parses");
        let mut scratch = document.to_vec();
        let parsed: Value = json::from_slice_mut(&mut scratch).expect("the in-place parse");
        assert_eq!(
            parsed,
            reference,
            "in place: {}",
            String::from_utf8_lossy(document)
        );
        let shared: Value = json::from_slice(document).expect("the shared parse");
        assert_eq!(shared, reference);
        let text: Value =
            json::from_str(std::str::from_utf8(document).expect("valid UTF-8")).expect("the text");
        assert_eq!(text, reference);
    }
}

#[test]
fn a_document_that_is_not_json_fails() {
    for document in NOT_JSON {
        assert!(
            serde_json::from_slice::<Value>(document).is_err(),
            "the reference accepted {:?}",
            String::from_utf8_lossy(document)
        );
        assert!(
            json::from_slice::<Value>(document).is_err(),
            "the facade accepted {:?}",
            String::from_utf8_lossy(document)
        );
    }
}

/// A typed record from a commit's stats blob: the shape the delta loader reads.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Stats {
    /// The log spells it `numRecords`.
    #[serde(rename = "numRecords")]
    record_count: u64,
    min_values: Option<Value>,
    tight_bounds: bool,
    #[serde(default)]
    absent: Option<String>,
}

#[test]
fn typing_a_record_matches_the_reference() {
    let document = STATS;
    let reference: Stats = serde_json::from_slice(document).expect("the reference parses");
    let mut scratch = document.to_vec();
    let parsed: Stats = json::from_slice_mut(&mut scratch).expect("the in-place parse");
    assert_eq!(parsed, reference);
    assert_eq!(parsed.record_count, 3);
    assert!(parsed.tight_bounds);
    assert_eq!(parsed.absent, None);
}

#[test]
fn writing_is_compact_and_newline_free() {
    let value = serde_json::json!({
        "name": "catalogs/c/schemas/s/tables/t/files/f.parquet",
        "size_bytes": 796,
        "ratio": 1.5,
    });
    let written = json::to_vec(&value).expect("the value writes");
    assert_eq!(written, serde_json::to_vec(&value).expect("the reference"));
    assert!(!written.contains(&b'\n'));
    let mut streamed = Vec::new();
    json::to_writer(&mut streamed, &value).expect("the value streams");
    assert_eq!(streamed, written);
}
