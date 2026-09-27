//! The backend-neutral model: a fenced code block and what its fence declares.
//!
//! The front end ([`crate::markdown`]) produces [`Block`]s; everything after
//! works on these, never on the tree-sitter tree.

/// A fenced code block lifted out of a Markdown document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// The fence info string as written, e.g. `console run` or `json`.
    pub info: BlockInfo,
    /// The block body, without the fences, verbatim.
    pub body: String,
    /// 1-based line of the opening fence.
    pub line: usize,
    /// 1-based line of the first body line.
    pub body_line: usize,
    /// Text of the nearest preceding heading, for naming a generated test.
    pub heading: Option<String>,
}

impl Block {
    /// The language tag, the first word of the info string. `None` for a bare
    /// fence.
    pub fn language(&self) -> Option<&str> {
        let language = self.info.language.as_deref()?;
        (!language.is_empty()).then_some(language)
    }

    /// Whether the fence asks the block to be run.
    pub fn is_runnable(&self) -> bool {
        self.info.run
    }
}

/// The parsed info string of a fence.
///
/// The first word is the language; the words after it are options. `run` marks
/// the block executable, `no-run` keeps an otherwise-likely block out, `json`
/// marks a block that intentionally shows NDJSON, and any other word is a cargo
/// feature the generated test is gated on (`console run delta` emits
/// `#[cfg(feature = "delta")]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BlockInfo {
    /// The language tag, or `None` for a bare fence.
    pub language: Option<String>,
    /// Whether `run` was one of the option words.
    pub run: bool,
    /// Whether `json` was one of the option words: the block documents NDJSON
    /// on purpose, so the no-NDJSON gate lets it through.
    pub json: bool,
    /// The other option words, in order, with `run`/`no-run`/`json` removed.
    /// Each is a cargo feature the generated test requires.
    pub options: Vec<String>,
}

impl BlockInfo {
    /// Parse a fence info string: `<language> <option>...`.
    pub fn parse(info: &str) -> Self {
        let mut words = info.split_whitespace();
        let language = words.next().map(str::to_owned);
        let mut run = false;
        let mut json = false;
        let mut options = Vec::new();
        for word in words {
            match word {
                "run" => run = true,
                // An explicit opt-out always wins, even if `run` was also given.
                "no-run" => run = false,
                // `json` is an exemption, not a cargo feature.
                "json" => json = true,
                other => options.push(other.to_owned()),
            }
        }
        Self {
            language,
            run,
            json,
            options,
        }
    }
}

/// A `#`-comment directive a block can carry on its first body line.
///
/// A directive is written as a shell comment so the block stays copy-pasteable:
///
/// ```text
/// # docscheck: env: PQB_ENDPOINT=https://example.cloud.databricks.com
/// # docscheck: cd: docker/e2e-lakehouse
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Directive {
    /// Set the working directory, relative to the repository root.
    Directory(String),
    /// Add an environment variable.
    Environment { name: String, value: String },
}

/// The directive prefix that marks a shell comment as documentation metadata.
pub const DIRECTIVE_PREFIX: &str = "# docscheck:";

impl Directive {
    /// Parse one directive line, or `None` if the line is not a directive.
    ///
    /// The line must start with [`DIRECTIVE_PREFIX`] after leading whitespace.
    pub fn parse(line: &str) -> Option<Self> {
        let rest = line.trim_start().strip_prefix(DIRECTIVE_PREFIX)?.trim();
        let (key, value) = rest.split_once(':')?;
        let value = value.trim();
        match key.trim() {
            "cd" if !value.is_empty() => Some(Directive::Directory(value.to_owned())),
            "env" => value
                .split_once('=')
                .map(|(name, value)| Directive::Environment {
                    name: name.trim().to_owned(),
                    value: value.to_owned(),
                }),
            _ => None,
        }
    }
}
