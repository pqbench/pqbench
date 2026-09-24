//! Hand-curated word data, kept apart from the rule logic.
//!
//! Two plain-text files hold every list: `words.txt` is the lexicon the tagger
//! consults, `rules.txt` is the smaller vocabulary the individual checks use.
//! A file is a sequence of sections; each starts with `# set:<name>` (one
//! token per line) or `# pairs:<name>` (two whitespace-separated columns).
//! `//` and blank lines are ignored. Editing the lexicon is editing these
//! files, not the checks.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

/// A two-column entry: a key and its replacement.
pub type Pair = (&'static str, &'static str);

struct Tables {
    sets: HashMap<&'static str, HashSet<&'static str>>,
    pairs: HashMap<&'static str, Vec<Pair>>,
    forward: HashMap<&'static str, HashMap<&'static str, &'static str>>,
    reverse: HashMap<&'static str, HashMap<&'static str, &'static str>>,
}

static TABLES: LazyLock<Tables> = LazyLock::new(parse);

fn parse() -> Tables {
    let mut sets: HashMap<&'static str, HashSet<&'static str>> = HashMap::new();
    let mut pairs: HashMap<&'static str, Vec<Pair>> = HashMap::new();
    let mut forward: HashMap<&'static str, HashMap<&'static str, &'static str>> = HashMap::new();
    let mut reverse: HashMap<&'static str, HashMap<&'static str, &'static str>> = HashMap::new();

    for text in [include_str!("words.txt"), include_str!("rules.txt")] {
        let mut section = "";
        let mut is_pairs = false;
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with("//") {
                continue;
            }
            if let Some(header) = line.strip_prefix('#') {
                let (kind, name) = header
                    .split_once(':')
                    .unwrap_or_else(|| panic!("section header without `kind:name`: `{header}`"));
                let name = name.trim();
                assert!(
                    !name.is_empty(),
                    "section header with an empty name: `{header}`"
                );
                section = name;
                is_pairs = match kind.trim() {
                    "set" => false,
                    "pairs" => true,
                    other => panic!("unknown section kind `{other}` in `{header}`"),
                };
                if is_pairs {
                    pairs.entry(section).or_default();
                    forward.entry(section).or_default();
                    reverse.entry(section).or_default();
                } else {
                    sets.entry(section).or_default();
                }
                continue;
            }
            assert!(
                !section.is_empty(),
                "word `{line}` before any section header"
            );
            if is_pairs {
                let (key, value) = line
                    .split_once(char::is_whitespace)
                    .unwrap_or_else(|| panic!("pair line without two columns: `{line}`"));
                let (key, value) = (key.trim(), value.trim());
                assert!(
                    !value.contains(char::is_whitespace),
                    "pair line with extra columns: `{line}`"
                );
                pairs.entry(section).or_default().push((key, value));
                forward.entry(section).or_default().insert(key, value);
                reverse.entry(section).or_default().insert(value, key);
            } else {
                sets.entry(section).or_default().insert(line);
            }
        }
    }

    Tables {
        sets,
        pairs,
        forward,
        reverse,
    }
}

/// The token set of a `# set:` section.
pub fn set(name: &str) -> &'static HashSet<&'static str> {
    TABLES
        .sets
        .get(name)
        .unwrap_or_else(|| panic!("unknown word set `{name}`"))
}

/// The key/value pairs of a `# pairs:` section, in file order.
pub fn pairs(name: &str) -> &'static [Pair] {
    TABLES
        .pairs
        .get(name)
        .unwrap_or_else(|| panic!("unknown pair set `{name}`"))
        .as_slice()
}

/// The value for `key` in a `# pairs:` section.
pub fn lookup(name: &str, key: &str) -> Option<&'static str> {
    TABLES
        .forward
        .get(name)
        .and_then(|map| map.get(key).copied())
}

/// The key whose value is `value` in a `# pairs:` section.
pub fn reverse_lookup(name: &str, value: &str) -> Option<&'static str> {
    TABLES
        .reverse
        .get(name)
        .and_then(|map| map.get(value).copied())
}
