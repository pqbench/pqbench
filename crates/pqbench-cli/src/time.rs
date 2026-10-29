//! Time arguments: the window a flag names, in milliseconds.
//!
//! `--last` is a rolling window ending now; `--partition` is one UTC day.
//! `list files` takes both, so the flags and their parsing live here.

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
    /// read only files the log dates in the last WINDOW, ending now: e.g. 1d,
    /// 1w
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
    let (number, unit) = value.split_at(
        value
            .len()
            .checked_sub(1)
            .ok_or_else(|| CliError::from(format!("{flag} is empty")))?,
    );
    let count: i64 = number
        .parse()
        .map_err(|_| CliError::from(format!("{flag} `{value}` needs a number and a unit")))?;
    if count <= 0 {
        return Err(format!("{flag} must be positive: {value}").into());
    }
    let millis = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return Err(format!("{flag} unit must be m, h, d, or w: {value}").into()),
    };
    Ok(count * millis)
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
    let mut parts = value.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(invalid());
    };
    if year.len() != 4
        || month.len() != 2
        || day.len() != 2
        || !year.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid());
    }
    let year: i64 = year.parse().map_err(|_| invalid())?;
    let month: u32 = month.parse().map_err(|_| invalid())?;
    let day: u32 = day.parse().map_err(|_| invalid())?;
    if month == 0 || month > 12 || day == 0 || day > month_days(year, month) {
        return Err(invalid());
    }
    let days = civil_days(year, month, day);
    let millis = u64::try_from(days)
        .map_err(|_| CliError::from(format!("--partition predates the Unix epoch: {value}")))?;
    Ok(millis * 86_400_000)
}

/// The days in `month` of `year`, Gregorian.
fn month_days(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

/// Days from 1970-01-01 to `year-month-day`, proleptic Gregorian (Howard
/// Hinnant's `days_from_civil`).
fn civil_days(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::civil_days;

    #[test]
    fn a_day_counts_from_the_epoch() {
        assert_eq!(civil_days(1970, 1, 1), 0);
        assert_eq!(civil_days(1970, 1, 2), 1);
        assert_eq!(civil_days(2026, 10, 28), 20_754);
        assert_eq!(civil_days(1969, 12, 31), -1);
        assert_eq!(civil_days(2000, 2, 29), 11_016);
    }
}
