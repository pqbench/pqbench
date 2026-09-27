//! Blackbox tests for the no-NDJSON gate: a documented output must be the
//! human table unless the fence opts into JSON.

use docscheck::{is_ndjson, markdown::parse_source, ndjson_outputs};

fn offenders(source: &str) -> Vec<docscheck::NdjsonStep> {
    ndjson_outputs(&parse_source(source))
}

#[test]
fn a_pqbench_record_is_ndjson() {
    assert!(is_ndjson(
        r#"{"kind":"pqbench.bytemass","version":1,"event":"begin"}"#
    ));
    assert!(is_ndjson(r#"  {"kind":"pqbench.profile","event":"end"}"#));
}

#[test]
fn a_table_line_is_not_ndjson() {
    assert!(!is_ndjson("id  INT64  UNCOMPRESSED  102  8"));
    assert!(!is_ndjson("file: examples/quickstart.parquet"));
    assert!(!is_ndjson(r#"{"other":"json"}"#));
}

#[test]
fn a_transcript_documenting_a_record_is_reported() {
    let source = "```console run\n$ pqbench skill\n\
                  {\"kind\":\"pqbench.skill\",\"name\":\"parquet-advisor\"}\n```\n";
    let found = offenders(source);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].line, 2);
    assert!(found[0].output.contains("pqbench.skill"));
}

#[test]
fn a_table_transcript_is_allowed() {
    let source = "```console run\n$ pqbench bytemass a.parquet --format table\n\
                  id  INT64  102  8\nfiles: 1\n```\n";
    assert!(offenders(source).is_empty());
}

#[test]
fn the_json_word_exempts_the_block() {
    let source = "```console run json\n$ pqbench skill\n\
                  {\"kind\":\"pqbench.skill\",\"name\":\"parquet-advisor\"}\n```\n";
    assert!(offenders(source).is_empty());
}

#[test]
fn a_no_run_block_is_not_scanned() {
    let source = "```console no-run\n$ pqbench skill\n\
                  {\"kind\":\"pqbench.skill\"}\n```\n";
    assert!(offenders(source).is_empty());
}

#[test]
fn the_json_word_is_not_a_cargo_feature() {
    let source = "```console run json\n$ pqbench skill\n\
                  {\"kind\":\"pqbench.skill\"}\n```\n";
    let blocks = parse_source(source);
    assert!(blocks[0].info.json);
    assert!(blocks[0].info.options.is_empty());
}
