//! Identifier word splitting and lightweight part-of-speech tagging.
//!
//! Splitting follows the research consensus: hard typographic boundaries
//! (separators, camel-case transitions, digits) resolve most identifiers, a
//! small known-token table keeps technical litterals (`utf8`, `sha256`) whole,
//! and anything ambiguous is left alone rather than guessed at.
//!
//! Tagging is lexicon-first and position-aware, matching what POSSE/SWUM did
//! with static analysis: the rightmost word of a name is its head noun,
//! non-final unknown words are noun modifiers, and `-ed`/`-ing` forms before
//! the head are modifiers rather than imperative verbs (`collected_items` is
//! good, `collect_items` is not). When no evidence points anywhere, the kind is
//! [`WordKind::Unknown`] and rules must stay quiet.
//!
//! The lexicon itself lives in the `data/` text files; this module holds only
//! the morphology and the positional rules over it.

use crate::data;

/// The lexical role a word plays in an identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WordKind {
    /// An action word, base or inflected (`render`, `renders`, `rendered`).
    Verb,
    /// A thing, including the likely head noun of the identifier.
    Noun,
    /// A noun used attributively (`report` in `report_row`).
    Modifier,
    /// A describing word (`total`, `partial`, `available`).
    Adjective,
    /// A closed-class relation word (`for`, `with`, `of`).
    Preposition,
    /// `a`, `the`, `each`, ...
    Determiner,
    /// `and`, `or`, `if`, ...
    Conjunction,
    /// A bare number (`2` in `level2`).
    Numeral,
    /// Not recognized by the lexicon or its morphology.
    Unknown,
}

/// One word of a split identifier, with its likely role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Word<'a> {
    /// The word as written.
    pub text: &'a str,
    /// The likely lexical role.
    pub kind: WordKind,
    /// Whether the word reads as a plural noun.
    pub plural: bool,
}

/// Whether a word is an unambiguous action verb: rarely also a noun in code,
/// so a leading or bare occurrence reads as an instruction (`collect_items`,
/// `disable`) rather than a state (`compress_durations`, `report_row`).
pub fn imperative_verb(word: &str) -> bool {
    data::set("imperative-verbs").contains(word.to_ascii_lowercase().as_str())
}

/// Split an identifier into its component words.
pub fn split_identifier(name: &str) -> Vec<&str> {
    let mut out = Vec::new();
    for segment in name.split(|c: char| !c.is_ascii_alphanumeric()) {
        if segment.is_empty() {
            continue;
        }
        if is_known_token(segment) {
            out.push(segment);
            continue;
        }
        split_segment(segment, &mut out);
    }
    out
}

/// Split an identifier and tag each word, using position as context.
pub fn tag_identifier(name: &str) -> Vec<Word<'_>> {
    let words = split_identifier(name);
    let last = words.len().saturating_sub(1);
    words
        .into_iter()
        .enumerate()
        .map(|(index, text)| {
            let mut kind = classify(text);
            if index != last {
                kind = match kind {
                    WordKind::Unknown => WordKind::Modifier,
                    WordKind::Verb if has_suffix(text, "ed") || has_suffix(text, "ing") => {
                        WordKind::Modifier
                    }
                    other => other,
                };
            } else if kind == WordKind::Unknown {
                kind = WordKind::Noun;
            }
            Word {
                text,
                kind,
                plural: is_plural(text),
            }
        })
        .collect()
}

/// Classify a single word without positional context.
pub fn classify(word: &str) -> WordKind {
    let lower = word.to_ascii_lowercase();
    if word.bytes().all(|b| b.is_ascii_digit()) {
        WordKind::Numeral
    } else if is_preposition(&lower) {
        WordKind::Preposition
    } else if is_determiner(&lower) {
        WordKind::Determiner
    } else if is_conjunction(&lower) {
        WordKind::Conjunction
    } else if is_verb_form(&lower) {
        WordKind::Verb
    } else if is_adjective(&lower) {
        WordKind::Adjective
    } else {
        WordKind::Unknown
    }
}

/// The plural form of a noun; uncountables and irregulars included.
pub fn plural(word: &str) -> String {
    let lower = word.to_ascii_lowercase();
    if is_uncountable(&lower) {
        return word.to_owned();
    }
    if let Some(form) = irregular_plural(&lower) {
        return form.to_owned();
    }
    if let Some(stem) = lower.strip_suffix("is") {
        return format!("{stem}es");
    }
    if has_suffix(&lower, "s")
        || has_suffix(&lower, "x")
        || has_suffix(&lower, "z")
        || has_suffix(&lower, "ch")
        || has_suffix(&lower, "sh")
    {
        return format!("{word}es");
    }
    if has_suffix(&lower, "y")
        && lower
            .chars()
            .rev()
            .nth(1)
            .is_some_and(|c| !matches!(c, 'a' | 'e' | 'i' | 'o' | 'u'))
    {
        return format!("{}ies", &word[..word.len() - 1]);
    }
    format!("{word}s")
}

/// The singular form of a noun; uncountables and irregulars included.
pub fn singular(word: &str) -> String {
    let lower = word.to_ascii_lowercase();
    if is_uncountable(&lower) || is_singular_s(&lower) {
        return word.to_owned();
    }
    if let Some(form) = irregular_singular(&lower) {
        return form.to_owned();
    }
    if let Some(stem) = lower.strip_suffix("ies") {
        return format!("{stem}y");
    }
    if has_suffix(&lower, "es")
        && (has_suffix(&lower, "ses") || has_suffix(&lower, "xes") || has_suffix(&lower, "zes"))
    {
        return word[..word.len() - 2].to_owned();
    }
    if has_suffix(&lower, "s") && !has_suffix(&lower, "ss") {
        return word[..word.len() - 1].to_owned();
    }
    word.to_owned()
}

/// Whether a noun reads as plural. Ambiguous words answer `false`.
pub fn is_plural(word: &str) -> bool {
    let lower = word.to_ascii_lowercase();
    if is_uncountable(&lower) || is_singular_s(&lower) {
        return false;
    }
    if irregular_singular(&lower).is_some() {
        return true;
    }
    let Some(stem) = strip_plural_suffix(&lower) else {
        return false;
    };
    plural(&stem) == lower
}

/// Whether a noun reads as singular. Ambiguous words answer `false`.
pub fn is_singular(word: &str) -> bool {
    let lower = word.to_ascii_lowercase();
    if is_uncountable(&lower) {
        return false;
    }
    if is_plural(&lower) {
        return false;
    }
    true
}

fn split_segment<'a>(segment: &'a str, out: &mut Vec<&'a str>) {
    let bytes = segment.as_bytes();
    let mut start = 0;
    for index in 1..bytes.len() {
        let prev = bytes[index - 1];
        let current = bytes[index];
        let boundary = digit_boundary(prev, current)
            || camel_boundary(prev, current)
            || acronym_boundary(prev, current, bytes.get(index + 1).copied());
        if boundary {
            out.push(&segment[start..index]);
            start = index;
        }
    }
    if start < segment.len() {
        out.push(&segment[start..]);
    }
}

fn digit_boundary(prev: u8, current: u8) -> bool {
    prev.is_ascii_digit() != current.is_ascii_digit()
}

fn camel_boundary(prev: u8, current: u8) -> bool {
    prev.is_ascii_lowercase() && current.is_ascii_uppercase()
}

fn acronym_boundary(prev: u8, current: u8, next: Option<u8>) -> bool {
    prev.is_ascii_uppercase()
        && current.is_ascii_uppercase()
        && next.is_some_and(|b| b.is_ascii_lowercase())
}

fn has_suffix(word: &str, suffix: &str) -> bool {
    word.len() > suffix.len() && word.ends_with(suffix)
}

fn strip_plural_suffix(word: &str) -> Option<String> {
    if let Some(stem) = word.strip_suffix("ies") {
        return Some(format!("{stem}y"));
    }
    if let Some(stem) = word.strip_suffix("es") {
        if has_suffix(stem, "s")
            || has_suffix(stem, "x")
            || has_suffix(stem, "z")
            || has_suffix(stem, "ch")
            || has_suffix(stem, "sh")
        {
            return Some(stem.to_owned());
        }
    }
    word.strip_suffix('s').map(str::to_owned)
}

fn is_known_token(word: &str) -> bool {
    data::set("known-tokens").contains(word.to_ascii_lowercase().as_str())
}

fn is_preposition(word: &str) -> bool {
    data::set("prepositions").contains(word)
}

fn is_determiner(word: &str) -> bool {
    data::set("determiners").contains(word)
}

fn is_conjunction(word: &str) -> bool {
    data::set("conjunctions").contains(word)
}

fn is_adjective(word: &str) -> bool {
    data::set("adjectives").contains(word)
        || data::set("adjective-suffixes")
            .iter()
            .any(|suffix| has_suffix(word, suffix))
}

/// Whether the word is a verb or an inflection of one.
fn is_verb_form(word: &str) -> bool {
    if is_verb(word) {
        return true;
    }
    if let Some(stem) = word.strip_suffix("ies") {
        if is_verb(&format!("{stem}y")) {
            return true;
        }
    }
    for suffix in ["ed", "ing"] {
        if let Some(stem) = word.strip_suffix(suffix) {
            if verb_stem(stem) {
                return true;
            }
        }
    }
    for suffix in ["es", "s"] {
        if let Some(stem) = word.strip_suffix(suffix) {
            if is_verb(stem) || is_verb(&format!("{stem}e")) {
                return true;
            }
        }
    }
    false
}

/// Whether a past/gerund stem recovers a base verb, undoing doubled
/// consonants (`stopped` -> `stop`) and `i`/`y` swaps (`studied` -> `study`).
fn verb_stem(stem: &str) -> bool {
    if is_verb(stem) {
        return true;
    }
    if is_verb(&format!("{stem}e")) {
        return true;
    }
    if stem.len() >= 2 && stem.ends_with('i') {
        let with_y = format!("{}y", &stem[..stem.len() - 1]);
        if is_verb(&with_y) {
            return true;
        }
    }
    let bytes = stem.as_bytes();
    if bytes.len() >= 3 && bytes[bytes.len() - 1] == bytes[bytes.len() - 2] {
        let deduped = &stem[..stem.len() - 1];
        return is_verb(deduped) || is_verb(&format!("{deduped}e"));
    }
    false
}

fn is_verb(word: &str) -> bool {
    data::set("verbs").contains(word)
}

fn is_uncountable(word: &str) -> bool {
    data::set("uncountables").contains(word)
}

/// Singular nouns that end in `s`; scanning for a plural suffix would misfire.
fn is_singular_s(word: &str) -> bool {
    data::set("singular-s").contains(word)
}

fn irregular_plural(singular: &str) -> Option<&'static str> {
    data::lookup("irregular-plurals", singular)
}

fn irregular_singular(plural: &str) -> Option<&'static str> {
    data::reverse_lookup("irregular-plurals", plural)
}
