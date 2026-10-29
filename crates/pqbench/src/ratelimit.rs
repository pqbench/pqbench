//! `ratelimit`: the catalog command's request pacer.
//!
//! One command talks to one catalog collection, so one bucket is the whole
//! pace: each request reserves the next slot and waits for it. [`Workspace`]
//! owns the one instance; storage reads take the shared HTTP client directly,
//! so object storage is not paced.
//!
//! [`Workspace`]: crate::Workspace

use std::time::{Duration, Instant};

/// The pacing state: when the next request may start.
#[derive(Debug)]
pub struct RateLimit {
    interval: Duration,
    next_time: Option<Instant>,
}

impl RateLimit {
    /// Pace requests at `requests_per_second`; `0` turns pacing off.
    #[must_use]
    pub fn new(requests_per_second: f64) -> Self {
        let interval = if requests_per_second.is_finite() && requests_per_second > 0.0 {
            Duration::from_secs_f64(1.0 / requests_per_second)
        } else {
            Duration::ZERO
        };
        Self {
            interval,
            next_time: None,
        }
    }

    /// Reserve the next slot at or after `now` and return how long to wait.
    /// Slots are not banked: a caller that arrives late starts from `now`.
    pub fn delay(&mut self, now: Instant) -> Duration {
        let start = now.max(self.next_time.unwrap_or(now));
        self.next_time = Some(start + self.interval);
        start.saturating_duration_since(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserves_one_slot_per_request() {
        let mut limit = RateLimit::new(10.0);
        let start = Instant::now();
        assert_eq!(limit.delay(start), Duration::ZERO);
        assert_eq!(limit.delay(start), Duration::from_millis(100));
        assert_eq!(limit.delay(start), Duration::from_millis(200));
    }

    #[test]
    fn a_late_caller_does_not_bank_slots() {
        let mut limit = RateLimit::new(10.0);
        let start = Instant::now();
        assert_eq!(limit.delay(start), Duration::ZERO);
        assert_eq!(limit.delay(start + Duration::from_secs(1)), Duration::ZERO);
        assert_eq!(
            limit.delay(start + Duration::from_secs(1)),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn zero_disables_pacing() {
        let mut limit = RateLimit::new(0.0);
        let start = Instant::now();
        assert_eq!(limit.delay(start), Duration::ZERO);
        assert_eq!(limit.delay(start), Duration::ZERO);
    }
}
