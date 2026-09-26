//! AIP-160 filtering over table files.
//!
//! `--filter` is one string: comparisons over file fields joined by `AND`,
//! `OR`, `NOT`, and parentheses, with quoted literals. The syntax follows
//! <https://google.aip.dev/160>; this is the subset pqbench needs. Filtering
//! runs as files stream out of a load, so a listing is never materialized, and
//! a path predicate can later prune a walk the way `find -prune` does.

use std::fmt;

use crate::table::TableFile;
use crate::third_party::chrono;

/// Errors from parsing a filter or an instant.
#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "filter: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// A parsed filter expression.
#[derive(Debug, Clone)]
pub struct Filter {
    root: Expr,
}

#[derive(Debug, Clone)]
enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    Compare(Compare),
}

#[derive(Debug, Clone)]
struct Compare {
    field: Field,
    op: Op,
    literal: Literal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    Path,
    Uri,
    SizeBytes,
    UpdateTime,
    SnapshotVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[derive(Debug, Clone)]
enum Literal {
    Text(String),
    Number(u64),
    Time(i64),
}

impl Filter {
    /// Parse an AIP-160 expression.
    ///
    /// # Errors
    /// Fails on an unknown field, a missing operator, a mismatched literal, or
    /// trailing input.
    pub fn parse(input: &str) -> Result<Self, Error> {
        let tokens = lex(input)?;
        if tokens.is_empty() {
            return Err(Error("empty filter".into()));
        }
        let mut parser = Parser {
            tokens: &tokens,
            pos: 0,
        };
        let root = parser.expr()?;
        if parser.pos != tokens.len() {
            return Err(Error("trailing input after filter".into()));
        }
        Ok(Self { root })
    }

    /// Whether `file` passes the filter. A field the file does not set never
    /// matches, even under `!=`.
    #[must_use]
    pub fn matches(&self, file: &TableFile) -> bool {
        eval(&self.root, file)
    }
}

fn eval(expr: &Expr, file: &TableFile) -> bool {
    match expr {
        Expr::And(parts) => parts.iter().all(|part| eval(part, file)),
        Expr::Or(parts) => parts.iter().any(|part| eval(part, file)),
        Expr::Not(inner) => !eval(inner, file),
        Expr::Compare(compare) => compare_file(compare, file),
    }
}

fn compare_file(compare: &Compare, file: &TableFile) -> bool {
    match (&compare.field, &compare.literal) {
        (Field::Path, Literal::Text(pattern)) => text(&file.path, compare.op, pattern),
        (Field::Uri, Literal::Text(pattern)) => text(&file.uri, compare.op, pattern),
        (Field::SizeBytes, Literal::Number(value)) => {
            number(file.size_bytes.into(), compare.op, (*value).into())
        }
        (Field::UpdateTime, Literal::Time(value)) => file
            .update_time
            .as_deref()
            .and_then(|time| chrono::parse_instant(time).ok())
            .is_some_and(|actual| number(actual.into(), compare.op, (*value).into())),
        (Field::SnapshotVersion, Literal::Number(value)) => file
            .snapshot_version
            .is_some_and(|actual| number(actual.into(), compare.op, (*value).into())),
        (_, _) => false,
    }
}

fn number(left: i128, op: Op, right: i128) -> bool {
    match op {
        Op::Eq => left == right,
        Op::Ne => left != right,
        Op::Lt => left < right,
        Op::Gt => left > right,
        Op::Le => left <= right,
        Op::Ge => left >= right,
    }
}

fn text(value: &str, op: Op, pattern: &str) -> bool {
    match op {
        Op::Eq => glob(pattern, value),
        Op::Ne => !glob(pattern, value),
        Op::Lt => value < pattern,
        Op::Gt => value > pattern,
        Op::Le => value <= pattern,
        Op::Ge => value >= pattern,
    }
}

/// Match `text` against a pattern where `*` matches any run of characters.
fn glob(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let mut table = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    table[0][0] = true;
    for (i, ch) in pattern.iter().enumerate() {
        if *ch == '*' {
            table[i + 1][0] = table[i][0];
        }
    }
    for i in 1..=pattern.len() {
        for j in 1..=text.len() {
            table[i][j] = if pattern[i - 1] == '*' {
                table[i - 1][j] || table[i][j - 1]
            } else {
                table[i - 1][j - 1] && pattern[i - 1] == text[j - 1]
            };
        }
    }
    table[pattern.len()][text.len()]
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Ident(String),
    Text(String),
    Number(u64),
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    And,
    Or,
    Not,
    LParen,
    RParen,
}

fn lex(input: &str) -> Result<Vec<Token>, Error> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&ch) = chars.peek() {
        match ch {
            c if c.is_whitespace() => {
                chars.next();
            }
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            '=' => {
                chars.next();
                tokens.push(Token::Eq);
            }
            '!' => {
                chars.next();
                if chars.next() != Some('=') {
                    return Err(Error("expected `!=`".into()));
                }
                tokens.push(Token::Ne);
            }
            '<' => {
                chars.next();
                tokens.push(if chars.peek() == Some(&'=') {
                    chars.next();
                    Token::Le
                } else {
                    Token::Lt
                });
            }
            '>' => {
                chars.next();
                tokens.push(if chars.peek() == Some(&'=') {
                    chars.next();
                    Token::Ge
                } else {
                    Token::Gt
                });
            }
            '-' => {
                chars.next();
                tokens.push(Token::Not);
            }
            '"' | '\'' => tokens.push(Token::Text(lex_text(&mut chars, ch)?)),
            c if c.is_ascii_digit() => tokens.push(Token::Number(lex_number(&mut chars)?)),
            c if c.is_ascii_alphabetic() || c == '_' => tokens.push(lex_word(&mut chars)),
            other => return Err(Error(format!("unexpected character `{other}`"))),
        }
    }
    Ok(tokens)
}

fn lex_text(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    quote: char,
) -> Result<String, Error> {
    chars.next();
    let mut text = String::new();
    for ch in chars.by_ref() {
        if ch == quote {
            return Ok(text);
        }
        text.push(ch);
    }
    Err(Error("unterminated string literal".into()))
}

fn lex_number(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<u64, Error> {
    let mut digits = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    digits
        .parse()
        .map_err(|_| Error(format!("invalid number `{digits}`")))
}

fn lex_word(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Token {
    let mut word = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            word.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    match word.to_ascii_lowercase().as_str() {
        "and" => Token::And,
        "or" => Token::Or,
        "not" => Token::Not,
        _ => Token::Ident(word),
    }
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl Parser<'_> {
    fn expr(&mut self) -> Result<Expr, Error> {
        self.and()
    }

    /// `AND` binds looser than `OR`, as AIP-160 specifies.
    fn and(&mut self) -> Result<Expr, Error> {
        let mut parts = vec![self.or()?];
        while self.eat(&Token::And) {
            parts.push(self.or()?);
        }
        Ok(collapse(parts, Expr::And))
    }

    fn or(&mut self) -> Result<Expr, Error> {
        let mut parts = vec![self.unary()?];
        while self.eat(&Token::Or) {
            parts.push(self.unary()?);
        }
        Ok(collapse(parts, Expr::Or))
    }

    fn unary(&mut self) -> Result<Expr, Error> {
        if self.eat(&Token::Not) {
            return Ok(Expr::Not(Box::new(self.unary()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr, Error> {
        if self.eat(&Token::LParen) {
            let inner = self.expr()?;
            self.expect(&Token::RParen)?;
            return Ok(inner);
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Result<Expr, Error> {
        let Some(Token::Ident(name)) = self.tokens.get(self.pos) else {
            return Err(Error("expected a field name".into()));
        };
        let field = Field::parse(name)?;
        self.pos += 1;
        let op = self.op()?;
        let literal = self.literal(field)?;
        Ok(Expr::Compare(Compare { field, op, literal }))
    }

    fn op(&mut self) -> Result<Op, Error> {
        let op = match self.tokens.get(self.pos) {
            Some(Token::Eq) => Op::Eq,
            Some(Token::Ne) => Op::Ne,
            Some(Token::Lt) => Op::Lt,
            Some(Token::Gt) => Op::Gt,
            Some(Token::Le) => Op::Le,
            Some(Token::Ge) => Op::Ge,
            _ => return Err(Error("expected a comparison operator".into())),
        };
        self.pos += 1;
        Ok(op)
    }

    fn literal(&mut self, field: Field) -> Result<Literal, Error> {
        let token = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        match (field, token) {
            (Field::Path | Field::Uri, Some(Token::Text(value))) => Ok(Literal::Text(value)),
            (Field::SizeBytes | Field::SnapshotVersion, Some(Token::Number(value))) => {
                Ok(Literal::Number(value))
            }
            (Field::UpdateTime, Some(Token::Text(value))) => Ok(Literal::Time(
                chrono::parse_instant(&value).map_err(|e| Error(e.to_string()))?,
            )),
            (_, Some(_)) => Err(Error(format!("literal does not match `{field}`"))),
            (_, None) => Err(Error("expected a literal".into())),
        }
    }

    fn eat(&mut self, token: &Token) -> bool {
        if self.tokens.get(self.pos) == Some(token) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, token: &Token) -> Result<(), Error> {
        if self.eat(token) {
            Ok(())
        } else {
            Err(Error(format!("expected {token:?}")))
        }
    }
}

fn collapse(mut parts: Vec<Expr>, combine: fn(Vec<Expr>) -> Expr) -> Expr {
    if parts.len() == 1 {
        parts.pop().expect("one part")
    } else {
        combine(parts)
    }
}

impl Field {
    fn parse(name: &str) -> Result<Self, Error> {
        match name {
            "path" => Ok(Self::Path),
            "uri" => Ok(Self::Uri),
            "size_bytes" => Ok(Self::SizeBytes),
            "update_time" => Ok(Self::UpdateTime),
            "snapshot_version" => Ok(Self::SnapshotVersion),
            other => Err(Error(format!(
                "unknown field `{other}`; expected path, uri, size_bytes, update_time, or snapshot_version"
            ))),
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Path => "path",
            Self::Uri => "uri",
            Self::SizeBytes => "size_bytes",
            Self::UpdateTime => "update_time",
            Self::SnapshotVersion => "snapshot_version",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, size: u64, time: Option<&str>, version: Option<u64>) -> TableFile {
        let mut file = TableFile::new(path, path, size);
        file.update_time = time.map(str::to_string);
        file.snapshot_version = version;
        file
    }

    #[test]
    fn filters_by_time_and_version() {
        let filter =
            Filter::parse("update_time >= \"2024-01-01\" AND snapshot_version >= 5").unwrap();
        assert!(filter.matches(&file("a.parquet", 1, Some("2024-06-01T00:00:00Z"), Some(5))));
        assert!(!filter.matches(&file("b.parquet", 1, Some("2023-12-31"), Some(5))));
        assert!(!filter.matches(&file("c.parquet", 1, Some("2024-06-01"), Some(4))));
    }

    #[test]
    fn filters_by_path_glob() {
        let filter = Filter::parse("path = \"year=2024/*\"").unwrap();
        assert!(filter.matches(&file("year=2024/part.parquet", 1, None, None)));
        assert!(!filter.matches(&file("year=2023/part.parquet", 1, None, None)));
    }

    #[test]
    fn an_unset_field_never_matches() {
        let filter = Filter::parse("update_time != \"2000-01-01\"").unwrap();
        assert!(!filter.matches(&file("a.parquet", 1, None, None)));
    }
    #[test]
    fn or_binds_tighter_than_and() {
        // AIP-160 parses this as `path = "x" AND (path = "x" OR size_bytes = 3)`,
        // not SQL's `(path = "x" AND path = "x") OR size_bytes = 3`.
        let filter = Filter::parse("path = \"x\" AND path = \"x\" OR size_bytes = 3").unwrap();
        assert!(filter.matches(&file("x", 9, None, None)));
        assert!(!filter.matches(&file("y", 3, None, None)));
    }

    #[test]
    fn unknown_field_is_an_error() {
        assert!(Filter::parse("nope = 1").is_err());
    }
}
