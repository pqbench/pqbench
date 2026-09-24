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
fn catches_the_historical_is_prefix_renames() {
    let source = "struct Bench {\n    pub is_json: bool,\n    pub is_d3: bool,\n    pub is_per_column: bool,\n}";
    let found = findings(source);
    assert_eq!(found.len(), 3);
    assert!(found
        .iter()
        .all(|finding| finding.rule == "aip-140/booleans"));
    let helps: Vec<&str> = found
        .iter()
        .filter_map(|finding| finding.help.as_deref())
        .collect();
    assert!(helps.contains(&"json"));
    assert!(helps.contains(&"d3"));
    assert!(helps.contains(&"per_column"));
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

    let found = findings("struct Bench { pub cfg: Config }");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].help.as_deref(), Some("`config`"));
}

#[test]
fn flags_prepositions_in_methods_but_skips_tests() {
    assert!(rules("impl Codec { pub fn impl_of(&self) -> u8 { 0 } }")
        .contains(&"aip-136/method-prepositions"));
    assert!(
        rules("impl Codec { #[test] fn reads_a_uri_through_the_api() {} }").is_empty(),
        "test functions are not API surface"
    );
}

#[test]
fn flags_reserved_words_in_fields() {
    assert!(rules("struct Book { pub r#new: String }").contains(&"aip-140/reserved-words"));
}

#[test]
fn flags_wrong_casing_per_kind() {
    let found = rules("pub struct report_row { pub TotalBytes: usize }");
    assert_eq!(
        found
            .iter()
            .filter(|rule| **rule == "aip-190/casing")
            .count(),
        2
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
fn agrees_singular_and_plural_with_collections() {
    assert!(rules("struct Shelf { pub books: Vec<Book> }").is_empty());
    assert!(rules("struct Shelf { pub book: Book }").is_empty());
    assert!(rules("struct Shelf { pub data: Vec<u8> }").is_empty());
    assert!(rules("struct Shelf { pub books: Option<Book> }").is_empty());

    let found = findings("struct Shelf { pub book: Vec<Book> }");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].rule, "aip-140/plural");

    let found = findings("struct Shelf { pub books: Book }");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].rule, "aip-140/plural");
}

#[test]
fn keeps_collective_nouns_and_type_named_fields_quiet() {
    // `log` reads the same in both numbers (AIP-144); `options: Options`
    // follows its own type name.
    assert!(rules("struct Table { pub log: Vec<Commit> }").is_empty());
    assert!(rules("struct Linter { pub options: Options }").is_empty());
}

#[test]
fn flags_abbreviated_units_and_count_prefixes() {
    assert!(rules("struct Report { pub throughput_mbps: f64 }").contains(&"aip-141/units"));
    assert!(rules("struct Report { pub width_px: f64 }").is_empty());

    let found = findings("struct Report { pub num_rows: usize }");
    let count: Vec<_> = found
        .iter()
        .filter(|finding| finding.rule == "aip-141/count-suffix")
        .collect();
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].help.as_deref(), Some("row_count"));
}

#[test]
fn flags_british_spellings() {
    let found = findings("struct Palette { pub colour: String }");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].rule, "aip-190/american-english");
    assert_eq!(found[0].help.as_deref(), Some("color"));
    assert!(rules("struct Palette { pub color: String }").is_empty());
}

#[test]
fn flags_async_in_names_but_not_test_names() {
    let found = findings("fn read_remote_async() {}");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].rule, "aip-136/async-name");
    assert_eq!(found[0].severity, Severity::ERROR);
    assert!(rules("#[test] fn reads_async() {}").is_empty());
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
        allowed_rules: vec!["aip-140/booleans".to_owned()],
        ..Options::default()
    };
    assert!(lint_source(source, &allowed)
        .iter()
        .all(|finding| finding.rule != "aip-140/booleans"));
}
