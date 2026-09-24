//! Blackbox tests for the first rule pack: snippets go in through the public
//! API and only the resulting findings are asserted.

use aipnaming::lint::{lint_source, Finding, Options, Severity};

fn findings(source: &str) -> Vec<Finding> {
    lint_source(source, &Options::default())
}

fn rules(source: &str) -> Vec<&'static str> {
    findings(source)
        .into_iter()
        .map(|finding| finding.rule)
        .collect()
}

#[test]
fn flags_boolean_verb_prefixes() {
    let found = findings("struct Page { pub is_dictionary: bool, pub dictionary: bool }");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].rule, "aip-140/booleans");
    assert_eq!(found[0].severity, Severity::ERROR);
    assert_eq!(found[0].help.as_deref(), Some("dictionary"));
}

#[test]
fn keeps_is_new_for_reserved_words() {
    assert!(rules("struct Page { pub is_new: bool }").is_empty());
}

#[test]
fn flags_prepositions_but_allows_conversion_prefixes() {
    assert!(rules("struct Book { pub reason_for_error: String }").contains(&"aip-140/prepositions"));
    assert!(
        rules("impl Codec { pub fn from_name(value: &str) -> Option<Codec> { None } }").is_empty()
    );
    assert!(rules("impl Book { pub fn into_pages(self) -> Vec<Page> { Vec::new() } }").is_empty());
}

#[test]
fn flags_fields_that_start_with_a_verb() {
    let found = findings("struct Report { pub collect_items: Vec<Item> }");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].rule, "aip-140/verbs");
    assert_eq!(found[0].help.as_deref(), Some("collected_items"));
    assert!(rules("struct Report { pub collected_items: Vec<Item> }").is_empty());
    assert!(rules("struct Report { pub report_row: String }").is_empty());
}

#[test]
fn flags_long_words_with_common_abbreviations() {
    let found = findings("struct Config { pub configuration: String }");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].rule, "aip-140/abbreviations");
    assert_eq!(found[0].help.as_deref(), Some("`config`"));
}

#[test]
fn flags_reserved_words_in_fields() {
    assert!(rules("struct Book { pub r#new: String }").contains(&"aip-140/reserved-words"));
}

#[test]
fn flags_wrong_casing_per_kind() {
    assert_eq!(
        rules("pub struct report_row { pub TotalBytes: usize }"),
        ["aip-190/casing", "aip-190/casing"]
    );
    assert!(rules("pub struct ReportRow { pub total_bytes: usize }").is_empty());
}

#[test]
fn accepts_both_rust_enum_value_styles() {
    let found = rules("enum Codec { Snappy, Zstd }");
    assert!(!found.contains(&"aip-190/casing"));
    let found = rules("enum Format { FORMAT_UNSPECIFIED, HARDBACK }");
    assert!(!found.contains(&"aip-190/casing"));
}

#[test]
fn flags_enum_without_a_zero_value() {
    let found = findings("enum Codec { Snappy, Zstd }");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].rule, "aip-126/unspecified");
    assert_eq!(found[0].help.as_deref(), Some("CODEC_UNSPECIFIED"));
}

#[test]
fn flags_non_imperative_and_wrong_suffixed_time_fields() {
    let found =
        rules("struct Book { pub created_at: u64, pub published_time: u64, pub timestamp: u64 }");
    let time_findings = found
        .iter()
        .filter(|rule| **rule == "aip-142/time-field-names")
        .count();
    assert_eq!(time_findings, 3);
}

#[test]
fn requires_documentation_on_offsets() {
    assert!(rules("struct Stream { pub start_offset: u64 }")
        .contains(&"aip-142/duration-offset-comment"));
    assert!(
        rules("struct Stream { /// Bytes into the segment.\n pub start_offset: u64 }").is_empty()
    );
}

#[test]
fn flags_min_max_pairs_as_ranges() {
    let found = findings("struct LevelRange { pub min_level: u8, pub max_level: u8 }");
    assert_eq!(found.len(), 2);
    assert!(found.iter().all(|finding| finding.rule == "aip-145/ranges"));
    assert!(rules("struct LevelRange { pub first_level: u8, pub last_level: u8 }").is_empty());
}

#[test]
fn honors_inline_allow_directives() {
    let source = "\
// aipnaming: allow(aip-140/booleans)
struct Page { pub is_dictionary: bool }
";
    assert!(rules(source).is_empty());
}

#[test]
fn options_can_select_and_allow_rules() {
    let source = "struct Page { pub is_dictionary: bool, pub min_bytes: u64, pub max_bytes: u64 }";
    let selected = Options {
        rules: vec!["aip-140/booleans".to_owned()],
        ..Options::default()
    };
    assert_eq!(lint_source(source, &selected).len(), 1);

    let allowed = Options {
        allow: vec!["aip-140/booleans".to_owned()],
        ..Options::default()
    };
    assert!(lint_source(source, &allowed)
        .iter()
        .all(|finding| finding.rule != "aip-140/booleans"));
}
