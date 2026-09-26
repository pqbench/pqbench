//! The time implementation, backed by the `chrono` crate.
//!
//! This module is private; callers use [`super::api`]. It is the only file that
//! names the `chrono` crate.

use ::chrono::{DateTime, NaiveDate, SecondsFormat};

use super::api::Error;

/// Parse `YYYY-MM-DD` or RFC3339 to epoch milliseconds.
pub(crate) fn parse_instant(value: &str) -> Result<i64, Error> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
        return Ok(parsed.timestamp_millis());
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .expect("00:00:00 is valid")
            .and_utc();
        return Ok(midnight.timestamp_millis());
    }
    Err(Error(format!(
        "invalid instant `{value}`; expected YYYY-MM-DD or RFC3339"
    )))
}

/// Format epoch milliseconds as RFC3339 UTC seconds, when in range.
pub(crate) fn format_instant(millis: i64) -> Option<String> {
    DateTime::from_timestamp_millis(millis)
        .map(|instant| instant.to_rfc3339_opts(SecondsFormat::Secs, true))
}
