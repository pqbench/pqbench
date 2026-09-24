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
    matches!(
        word.to_ascii_lowercase().as_str(),
        "aggregate"
            | "calculate"
            | "collect"
            | "compute"
            | "concatenate"
            | "convert"
            | "decode"
            | "deserialize"
            | "disable"
            | "dispatch"
            | "enable"
            | "encode"
            | "escape"
            | "execute"
            | "flatten"
            | "generate"
            | "initialize"
            | "iterate"
            | "marshal"
            | "normalize"
            | "notify"
            | "optimize"
            | "parse"
            | "produce"
            | "render"
            | "serialize"
            | "transform"
            | "translate"
            | "traverse"
            | "truncate"
            | "unmarshal"
            | "validate"
            | "verify"
    )
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
    matches!(
        word.to_ascii_lowercase().as_str(),
        "utf8"
            | "utf16"
            | "utf32"
            | "sha1"
            | "sha224"
            | "sha256"
            | "sha384"
            | "sha512"
            | "sha3"
            | "md5"
            | "base16"
            | "base32"
            | "base64"
            | "ipv4"
            | "ipv6"
            | "mp3"
            | "mp4"
            | "h264"
            | "h265"
            | "h266"
            | "oauth2"
            | "http2"
            | "http3"
            | "tls12"
            | "tls13"
            | "i18n"
            | "l10n"
            | "k8s"
            | "p2p"
            | "argon2"
            | "aes128"
            | "aes256"
            | "rsa2048"
            | "rsa4096"
    )
}

fn is_preposition(word: &str) -> bool {
    matches!(
        word,
        "about"
            | "above"
            | "across"
            | "after"
            | "against"
            | "along"
            | "among"
            | "around"
            | "at"
            | "before"
            | "behind"
            | "below"
            | "beneath"
            | "beside"
            | "between"
            | "beyond"
            | "by"
            | "despite"
            | "down"
            | "during"
            | "except"
            | "for"
            | "from"
            | "in"
            | "inside"
            | "into"
            | "near"
            | "of"
            | "off"
            | "on"
            | "onto"
            | "outside"
            | "over"
            | "past"
            | "since"
            | "than"
            | "through"
            | "throughout"
            | "till"
            | "to"
            | "toward"
            | "towards"
            | "under"
            | "underneath"
            | "until"
            | "up"
            | "upon"
            | "via"
            | "with"
            | "within"
            | "without"
    )
}

fn is_determiner(word: &str) -> bool {
    matches!(
        word,
        "a" | "an"
            | "the"
            | "this"
            | "that"
            | "these"
            | "those"
            | "each"
            | "every"
            | "either"
            | "neither"
            | "some"
            | "any"
            | "no"
            | "all"
            | "both"
            | "few"
            | "many"
            | "much"
            | "more"
            | "most"
            | "other"
            | "another"
            | "such"
            | "own"
            | "same"
    )
}

fn is_conjunction(word: &str) -> bool {
    matches!(
        word,
        "and"
            | "or"
            | "but"
            | "nor"
            | "so"
            | "yet"
            | "if"
            | "then"
            | "because"
            | "while"
            | "when"
            | "where"
            | "whether"
            | "though"
            | "although"
            | "unless"
            | "else"
    )
}

fn is_adjective(word: &str) -> bool {
    if matches!(
        word,
        "active"
            | "available"
            | "bare"
            | "blank"
            | "broken"
            | "busy"
            | "clean"
            | "closed"
            | "cold"
            | "compact"
            | "complete"
            | "complex"
            | "constant"
            | "current"
            | "dense"
            | "default"
            | "deterministic"
            | "dirty"
            | "double"
            | "dynamic"
            | "empty"
            | "enabled"
            | "exact"
            | "exclusive"
            | "external"
            | "final"
            | "fixed"
            | "flat"
            | "free"
            | "fresh"
            | "full"
            | "global"
            | "half"
            | "hard"
            | "heavy"
            | "high"
            | "hot"
            | "immutable"
            | "inactive"
            | "incomplete"
            | "initial"
            | "inner"
            | "internal"
            | "invalid"
            | "large"
            | "last"
            | "lazy"
            | "local"
            | "long"
            | "loose"
            | "low"
            | "mutable"
            | "narrow"
            | "new"
            | "next"
            | "old"
            | "open"
            | "optional"
            | "outer"
            | "owned"
            | "parallel"
            | "partial"
            | "pending"
            | "plain"
            | "previous"
            | "private"
            | "public"
            | "random"
            | "raw"
            | "ready"
            | "remote"
            | "required"
            | "rough"
            | "safe"
            | "sequential"
            | "shared"
            | "short"
            | "simple"
            | "single"
            | "slow"
            | "small"
            | "smooth"
            | "sparse"
            | "stable"
            | "stale"
            | "static"
            | "strict"
            | "strong"
            | "synchronous"
            | "temporary"
            | "total"
            | "unique"
            | "unsafe"
            | "unstable"
            | "valid"
            | "verbose"
            | "weak"
            | "wide"
            | "whole"
    ) {
        return true;
    }
    [
        "able", "ible", "ative", "ive", "ical", "ic", "ious", "eous", "ous", "iful", "ful", "less",
        "ish", "like",
    ]
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
    matches!(
        word,
        "abort"
            | "accept"
            | "access"
            | "accumulate"
            | "acquire"
            | "add"
            | "adjust"
            | "aggregate"
            | "allocate"
            | "analyze"
            | "append"
            | "apply"
            | "archive"
            | "assert"
            | "assign"
            | "attach"
            | "authenticate"
            | "await"
            | "begin"
            | "belong"
            | "bench"
            | "bind"
            | "borrow"
            | "box"
            | "break"
            | "broadcast"
            | "build"
            | "calculate"
            | "call"
            | "cancel"
            | "capture"
            | "cast"
            | "catch"
            | "change"
            | "check"
            | "choose"
            | "clean"
            | "clear"
            | "clone"
            | "close"
            | "collect"
            | "combine"
            | "compare"
            | "compile"
            | "complete"
            | "compose"
            | "compress"
            | "compute"
            | "concat"
            | "configure"
            | "connect"
            | "consume"
            | "contain"
            | "continue"
            | "convert"
            | "copy"
            | "count"
            | "create"
            | "decode"
            | "decompress"
            | "decrement"
            | "delete"
            | "deny"
            | "depend"
            | "dequeue"
            | "derive"
            | "describe"
            | "deserialize"
            | "detach"
            | "detect"
            | "determine"
            | "diff"
            | "disable"
            | "disconnect"
            | "dispatch"
            | "display"
            | "dispose"
            | "divide"
            | "download"
            | "draw"
            | "drop"
            | "dump"
            | "emit"
            | "enable"
            | "encode"
            | "enqueue"
            | "ensure"
            | "enter"
            | "enumerate"
            | "equal"
            | "estimate"
            | "evaluate"
            | "exclude"
            | "execute"
            | "exist"
            | "expand"
            | "expect"
            | "export"
            | "extract"
            | "fail"
            | "fetch"
            | "filter"
            | "find"
            | "finish"
            | "flatten"
            | "flush"
            | "fold"
            | "format"
            | "forward"
            | "free"
            | "generate"
            | "get"
            | "group"
            | "grow"
            | "handle"
            | "hash"
            | "hide"
            | "highlight"
            | "include"
            | "increment"
            | "index"
            | "infer"
            | "init"
            | "initialize"
            | "inject"
            | "insert"
            | "inspect"
            | "install"
            | "invert"
            | "invoke"
            | "iterate"
            | "join"
            | "keep"
            | "lend"
            | "link"
            | "list"
            | "load"
            | "lock"
            | "log"
            | "lookup"
            | "make"
            | "map"
            | "mark"
            | "match"
            | "measure"
            | "merge"
            | "mount"
            | "move"
            | "multiply"
            | "name"
            | "normalize"
            | "notify"
            | "observe"
            | "open"
            | "optimize"
            | "order"
            | "pack"
            | "panic"
            | "parse"
            | "partition"
            | "patch"
            | "peek"
            | "poll"
            | "pop"
            | "prepare"
            | "print"
            | "process"
            | "produce"
            | "project"
            | "publish"
            | "pull"
            | "push"
            | "put"
            | "query"
            | "read"
            | "receive"
            | "recognize"
            | "recover"
            | "reduce"
            | "refactor"
            | "register"
            | "reject"
            | "release"
            | "reload"
            | "remove"
            | "rename"
            | "render"
            | "repeat"
            | "replace"
            | "report"
            | "request"
            | "require"
            | "reset"
            | "resolve"
            | "respond"
            | "restore"
            | "resume"
            | "retry"
            | "return"
            | "revert"
            | "rollback"
            | "run"
            | "sample"
            | "save"
            | "scan"
            | "schedule"
            | "search"
            | "seek"
            | "select"
            | "send"
            | "serialize"
            | "serve"
            | "set"
            | "share"
            | "shift"
            | "show"
            | "shrink"
            | "shutdown"
            | "sign"
            | "skip"
            | "sleep"
            | "slice"
            | "sort"
            | "spawn"
            | "split"
            | "start"
            | "stop"
            | "store"
            | "stream"
            | "strip"
            | "submit"
            | "subscribe"
            | "sum"
            | "summarize"
            | "swap"
            | "synchronize"
            | "take"
            | "test"
            | "throw"
            | "toggle"
            | "trace"
            | "transform"
            | "translate"
            | "transmit"
            | "traverse"
            | "trim"
            | "truncate"
            | "try"
            | "unbind"
            | "unbox"
            | "unlink"
            | "unlock"
            | "unmount"
            | "unpack"
            | "unregister"
            | "update"
            | "upload"
            | "use"
            | "validate"
            | "verify"
            | "visit"
            | "wait"
            | "walk"
            | "warn"
            | "watch"
            | "wrap"
            | "write"
            | "yield"
    )
}

fn is_uncountable(word: &str) -> bool {
    matches!(
        word,
        "advice"
            | "air"
            | "audio"
            | "cash"
            | "content"
            | "data"
            | "equipment"
            | "feedback"
            | "firmware"
            | "hardware"
            | "health"
            | "info"
            | "information"
            | "knowledge"
            | "media"
            | "metadata"
            | "moose"
            | "news"
            | "payload"
            | "research"
            | "series"
            | "sheep"
            | "software"
            | "species"
            | "stuff"
            | "traffic"
            | "weather"
    )
}

/// Singular nouns that end in `s`; scanning for a plural suffix would misfire.
fn is_singular_s(word: &str) -> bool {
    matches!(
        word,
        "access"
            | "address"
            | "alias"
            | "analysis"
            | "atlas"
            | "axis"
            | "basis"
            | "bias"
            | "bus"
            | "canvas"
            | "campus"
            | "census"
            | "class"
            | "corpus"
            | "crisis"
            | "cross"
            | "focus"
            | "gas"
            | "glass"
            | "index"
            | "lens"
            | "loss"
            | "mass"
            | "matrix"
            | "pass"
            | "press"
            | "process"
            | "progress"
            | "radius"
            | "status"
            | "stress"
            | "success"
            | "thesis"
            | "virus"
    )
}

fn irregular_plural(singular: &str) -> Option<&'static str> {
    Some(match singular {
        "alumnus" => "alumni",
        "analysis" => "analyses",
        "appendix" => "appendices",
        "axis" => "axes",
        "bacterium" => "bacteria",
        "basis" => "bases",
        "cactus" => "cacti",
        "calf" => "calves",
        "child" => "children",
        "criterion" => "criteria",
        "crisis" => "crises",
        "curriculum" => "curricula",
        "datum" => "data",
        "die" => "dice",
        "focus" => "foci",
        "foot" => "feet",
        "fungus" => "fungi",
        "goose" => "geese",
        "half" => "halves",
        "hero" => "heroes",
        "hypothesis" => "hypotheses",
        "index" => "indices",
        "knife" => "knives",
        "leaf" => "leaves",
        "life" => "lives",
        "loaf" => "loaves",
        "louse" => "lice",
        "man" => "men",
        "matrix" => "matrices",
        "medium" => "media",
        "memorandum" => "memoranda",
        "mouse" => "mice",
        "nucleus" => "nuclei",
        "ox" => "oxen",
        "parenthesis" => "parentheses",
        "person" => "people",
        "phenomenon" => "phenomena",
        "potato" => "potatoes",
        "quiz" => "quizzes",
        "radius" => "radii",
        "scarf" => "scarves",
        "self" => "selves",
        "shelf" => "shelves",
        "stimulus" => "stimuli",
        "syllabus" => "syllabi",
        "thesis" => "theses",
        "thief" => "thieves",
        "tooth" => "teeth",
        "tomato" => "tomatoes",
        "vertex" => "vertices",
        "veto" => "vetoes",
        "wharf" => "wharves",
        "wife" => "wives",
        "wolf" => "wolves",
        "woman" => "women",
        _ => return None,
    })
}

fn irregular_singular(plural: &str) -> Option<&'static str> {
    Some(match plural {
        "alumni" => "alumnus",
        "analyses" => "analysis",
        "appendices" => "appendix",
        "axes" => "axis",
        "bacteria" => "bacterium",
        "bases" => "basis",
        "cacti" => "cactus",
        "calves" => "calf",
        "children" => "child",
        "criteria" => "criterion",
        "crises" => "crisis",
        "curricula" => "curriculum",
        "data" => "datum",
        "dice" => "die",
        "feet" => "foot",
        "foci" => "focus",
        "fungi" => "fungus",
        "geese" => "goose",
        "halves" => "half",
        "heroes" => "hero",
        "hypotheses" => "hypothesis",
        "indices" => "index",
        "knives" => "knife",
        "leaves" => "leaf",
        "lice" => "louse",
        "lives" => "life",
        "loaves" => "loaf",
        "matrices" => "matrix",
        "media" => "medium",
        "memoranda" => "memorandum",
        "men" => "man",
        "mice" => "mouse",
        "nuclei" => "nucleus",
        "oxen" => "ox",
        "parentheses" => "parenthesis",
        "people" => "person",
        "phenomena" => "phenomenon",
        "potatoes" => "potato",
        "quizzes" => "quiz",
        "radii" => "radius",
        "scarves" => "scarf",
        "selves" => "self",
        "shelves" => "shelf",
        "stimuli" => "stimulus",
        "syllabi" => "syllabus",
        "teeth" => "tooth",
        "theses" => "thesis",
        "thieves" => "thief",
        "tomatoes" => "tomato",
        "vertices" => "vertex",
        "vetoes" => "veto",
        "wharves" => "wharf",
        "wives" => "wife",
        "wolves" => "wolf",
        "women" => "woman",
        _ => return None,
    })
}
