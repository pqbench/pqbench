//! Time arguments: the window a flag names, in milliseconds.
//!
//! `--last` is a rolling window ending now; `--partition` is one UTC day.
//! `list files` takes both, so the flags and their parsing live here.

use chrono::{NaiveDate, NaiveTime};
use clap::Args;
use pqbench::time::TimeWindow;

use crate::CliError;

/// The window flags, shared by the commands that can narrow a file set.
#[derive(Args)]
pub(crate) struct WindowArgs {
    /// read only files the log dates in one UTC day: an ISO date, YYYY-MM-DD;
    /// e.g. 2026-10-28
    #[arg(
        long,
        value_parser = parse_day,
        value_name = "DATE",
        conflicts_with = "last"
    )]
    partition: Option<u64>,
    /// read only files the log dates in the last WINDOW, ending now: e.g. 1m,
    /// 1h, 1d, 1w
    #[arg(long, value_parser = parse_last, value_name = "WINDOW")]
    last: Option<u64>,
}

impl WindowArgs {
    /// The window the flags name, if any.
    pub(crate) fn window(&self) -> Option<TimeWindow> {
        match (self.partition, self.last) {
            (Some(day), _) => Some(TimeWindow::day(day)),
            (None, Some(last)) => {
                let now = now_millis();
                Some(TimeWindow {
                    first_time: now.saturating_sub(last),
                    last_time: now,
                })
            }
            (None, None) => None,
        }
    }
}

/// Parse a window: a positive integer and a unit (`m`, `h`, `d`, `w`).
pub(crate) fn parse_window(flag: &str, value: &str) -> Result<i64, CliError> {
    let mut chars = value.chars();
    let unit = chars
        .next_back()
        .ok_or_else(|| CliError::from(format!("{flag} is empty")))?;
    let number = chars.as_str();
    let count: i64 = number
        .parse()
        .map_err(|_| CliError::from(format!("{flag} `{value}` needs a number and a unit")))?;
    if count <= 0 {
        return Err(format!("{flag} must be positive: {value}").into());
    }
    let millis: i64 = match unit {
        'm' => 60_000,
        'h' => 3_600_000,
        'd' => 86_400_000,
        'w' => 604_800_000,
        _ => return Err(format!("{flag} unit must be m, h, d, or w: {value}").into()),
    };
    count
        .checked_mul(millis)
        .ok_or_else(|| CliError::from(format!("{flag} is out of range: {value}")))
}

/// Parse `--last`: a positive window, in milliseconds.
fn parse_last(value: &str) -> Result<u64, CliError> {
    let millis = parse_window("--last", value)?;
    u64::try_from(millis).map_err(|_| CliError::from(format!("--last is out of range: {value}")))
}

/// The current time, milliseconds since the Unix epoch.
pub(crate) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Parse `--partition`: an ISO date, `YYYY-MM-DD`, as the first millisecond of
/// the UTC day it names.
fn parse_day(value: &str) -> Result<u64, CliError> {
    let invalid = || {
        CliError::from(format!(
            "--partition needs an ISO date, YYYY-MM-DD: {value}"
        ))
    };
    if !iso_date_shape(value) {
        return Err(invalid());
    }
    // chrono owns the civil calendar: the month's length, the leap day, and
    // which years have one.
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| invalid())?;
    let millis = date.and_time(NaiveTime::MIN).and_utc().timestamp_millis();
    u64::try_from(millis)
        .map_err(|_| CliError::from(format!("--partition predates the Unix epoch: {value}")))
}

/// Whether `value` is written `YYYY-MM-DD`: four year, two month, and two day
/// digits around two dashes. chrono would also read a one-digit month or a
/// signed year, which is not the form the flag documents.
fn iso_date_shape(value: &str) -> bool {
    value.len() == 10
        && value
            .as_bytes()
            .iter()
            .enumerate()
            .all(|(at, byte)| match at {
                4 | 7 => *byte == b'-',
                _ => byte.is_ascii_digit(),
            })
}

#[cfg(test)]
mod tests {
    use super::{parse_day, parse_window};

    #[test]
    fn a_day_counts_from_the_epoch() {
        assert_eq!(parse_day("1970-01-01").unwrap(), 0);
        assert_eq!(parse_day("1970-01-02").unwrap(), 86_400_000);
        assert_eq!(parse_day("2026-10-28").unwrap(), 20_754 * 86_400_000);
        assert_eq!(parse_day("2000-02-29").unwrap(), 11_016 * 86_400_000);
        // A day before the epoch has no millisecond count to report.
        assert!(parse_day("1969-12-31").is_err());
    }

    /// Only `YYYY-MM-DD` is a date: a wrong width, a separator, or a day the
    /// month does not have is refused with the same error.
    #[test]
    fn a_date_that_is_not_iso_is_refused() {
        for value in [
            "",
            "2026-1-1",
            "26-10-28",
            "2026/10/28",
            "2026-13-01",
            "2026-02-30",
            "2026-10-28T00:00:00Z",
        ] {
            let error = parse_day(value).expect_err("not an ISO date");
            assert!(
                error
                    .to_string()
                    .contains("--partition needs an ISO date, YYYY-MM-DD"),
                "{error}"
            );
        }
    }

    #[test]
    fn malformed_and_overflowing_windows_error_without_panicking() {
        assert!(parse_window("--last", "").is_err());
        assert!(parse_window("--last", "1💥").is_err());
        assert!(parse_window("--last", "9223372036854775807h").is_err());
        assert!(parse_window("--last", "0h").is_err());
        assert_eq!(parse_window("--last", "2h").unwrap(), 7_200_000);
    }
}
