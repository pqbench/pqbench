//! A window of file modification times: the filter a daily or weekly run uses.

/// A half-open window of file modification times, milliseconds since the Unix
/// epoch: `first_time` inclusive, `last_time` exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindow {
    /// The first millisecond the window holds.
    pub first_time: u64,
    /// The millisecond the window ends at, excluded.
    pub last_time: u64,
}

impl TimeWindow {
    /// The UTC day starting at `first_time`, one day long.
    #[must_use]
    pub fn day(first_time: u64) -> Self {
        Self {
            first_time,
            last_time: first_time + 86_400_000,
        }
    }

    /// Whether a file's logged time falls in the window.
    ///
    /// A file whose log records no time cannot be shown to be in the window, so
    /// it is excluded.
    #[must_use]
    pub fn contains(&self, modification_time: Option<u64>) -> bool {
        modification_time.is_some_and(|time| (self.first_time..self.last_time).contains(&time))
    }
}
