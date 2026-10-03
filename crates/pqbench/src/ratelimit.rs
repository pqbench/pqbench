//! `ratelimit`: pace an NDJSON ref stream to a target rate.
//!
//! A filter: records come in on standard input and go out unchanged, delayed
//! so each record `kind` observes at most N records per second. One bucket
//! per kind, so a mixed stream paces catalogs, schemas, and table refs
//! independently — the walk's per-endpoint pace. Nothing is dropped.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Records per second each kind is paced at by default.
pub const DEFAULT_REQUESTS_PER_SECOND: f64 = 15.0;

/// The pacing state: one next-slot time per record kind.
#[derive(Debug)]
pub struct RateLimit {
    interval: Duration,
    next_times: BTreeMap<String, Instant>,
}

impl RateLimit {
    /// Pace each kind at `requests_per_second`; `0` turns pacing off.
    #[must_use]
    pub fn new(requests_per_second: f64) -> Self {
        let interval = if requests_per_second > 0.0 {
            Duration::from_secs_f64(1.0 / requests_per_second)
        } else {
            Duration::ZERO
        };
        Self {
            interval,
            next_times: BTreeMap::new(),
        }
    }

    /// Reserve `kind`'s next slot at or after `now` and return how long to
    /// wait for it. Kinds are independent, so one slow kind never delays
    /// another.
    pub fn delay(&mut self, kind: &str, now: Instant) -> Duration {
        let next = match self.next_times.get_mut(kind) {
            Some(next) => next,
            None => self.next_times.entry(kind.to_string()).or_insert(now),
        };
        let start = now.max(*next);
        *next = start + self.interval;
        start.saturating_duration_since(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_pace_independently() {
        let mut limit = RateLimit::new(10.0);
        let start = Instant::now();
        assert_eq!(limit.delay("catalog", start), Duration::ZERO);
        assert_eq!(limit.delay("schema", start), Duration::ZERO);
        assert_eq!(limit.delay("catalog", start), Duration::from_millis(100));
        assert_eq!(limit.delay("schema", start), Duration::from_millis(100));
        assert_eq!(
            limit.delay("catalog", start + Duration::from_millis(250)),
            Duration::ZERO
        );
    }

    #[test]
    fn zero_disables_pacing() {
        let mut limit = RateLimit::new(0.0);
        let start = Instant::now();
        assert_eq!(limit.delay("catalog", start), Duration::ZERO);
        assert_eq!(limit.delay("catalog", start), Duration::ZERO);
    }
}
