//! Blackbox tests for the word layer: splitting, tagging, morphology.

use aipnaming::words::{
    imperative_verb, is_plural, is_singular, plural, singular, split_identifier, tag_identifier,
    WordKind,
};

fn kinds(name: &str) -> Vec<WordKind> {
    tag_identifier(name)
        .into_iter()
        .map(|word| word.kind)
        .collect()
}

#[test]
fn splits_on_separators_camel_case_acronyms_and_digits() {
    assert_eq!(split_identifier("read_masses"), ["read", "masses"]);
    assert_eq!(split_identifier("ReadMasses"), ["Read", "Masses"]);
    assert_eq!(split_identifier("HTTPServer"), ["HTTP", "Server"]);
    assert_eq!(split_identifier("utf8_octets"), ["utf8", "octets"]);
    assert_eq!(split_identifier("sha256_hash"), ["sha256", "hash"]);
    assert_eq!(split_identifier("level2_report"), ["level", "2", "report"]);
    assert_eq!(split_identifier("nbrOfBugs"), ["nbr", "Of", "Bugs"]);
}

#[test]
fn tags_verbs_nouns_modifiers_and_prepositions() {
    assert_eq!(kinds("collect_items"), [WordKind::Verb, WordKind::Noun]);
    assert_eq!(
        kinds("collected_items"),
        [WordKind::Modifier, WordKind::Noun]
    );
    assert_eq!(kinds("waiting_list"), [WordKind::Modifier, WordKind::Verb]);
    assert_eq!(
        kinds("reason_for_error"),
        [WordKind::Modifier, WordKind::Preposition, WordKind::Noun]
    );
    assert_eq!(kinds("total_bytes"), [WordKind::Adjective, WordKind::Noun]);
}

#[test]
fn marks_the_head_noun_plural() {
    let words = tag_identifier("compressed_bytes");
    assert_eq!(words.last().unwrap().kind, WordKind::Noun);
    assert!(words.last().unwrap().plural);
}

#[test]
fn only_unambiguous_verbs_count_as_imperative() {
    assert!(imperative_verb("collect"));
    assert!(imperative_verb("render"));
    assert!(!imperative_verb("report"));
    assert!(!imperative_verb("filter"));
    assert!(!imperative_verb("count"));
}

#[test]
fn pluralizes_regular_and_irregular_nouns() {
    assert_eq!(plural("box"), "boxes");
    assert_eq!(plural("byte"), "bytes");
    assert_eq!(plural("entry"), "entries");
    assert_eq!(plural("index"), "indices");
    assert_eq!(plural("person"), "people");
    assert_eq!(plural("class"), "classes");
}

#[test]
fn singularizes_regular_and_irregular_nouns() {
    assert_eq!(singular("bytes"), "byte");
    assert_eq!(singular("entries"), "entry");
    assert_eq!(singular("indices"), "index");
    assert_eq!(singular("people"), "person");
    assert_eq!(singular("status"), "status");
    assert_eq!(singular("classes"), "class");
}

#[test]
fn plural_and_singular_agree_with_ambiguity_silence() {
    assert!(is_plural("bytes"));
    assert!(is_plural("people"));
    assert!(is_plural("entries"));
    assert!(!is_plural("status"));
    assert!(is_singular("byte"));
    assert!(is_singular("index"));
    assert!(!is_plural("data"));
    assert!(!is_singular("data"));
}
