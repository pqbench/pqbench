//! Parsing and formatting of instants, without exposing a `chrono` type.

/// Format epoch milliseconds as RFC3339 UTC seconds, when in range.
#[must_use]
pub fn format_instant(millis: i64) -> Option<String> {
    super::r#impl::format_instant(millis)
}

/// Errors from the time layer.
#[derive(Debug)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "time: {}", self.0)
    }
}

impl std::error::Error for Error {}

/// Parse `YYYY-MM-DD` or RFC3339 to epoch milliseconds.
///
/// # Errors
/// Returns [`Error`] when `value` is neither form.
pub fn parse_instant(value: &str) -> Result<i64, Error> {
    super::r#impl::parse_instant(value)
}
