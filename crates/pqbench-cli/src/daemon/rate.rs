//! The daemon's pacer for Unity Catalog vending calls.
//!
//! A walk can have many tables in flight and every table needs a vend. Unity
//! rate-limits its REST endpoints at about fifteen requests a second, so the
//! daemon spaces vends at that rate. One vend is one `GET /tables/{name}` and
//! one `POST /temporary-table-credentials`, each on its own endpoint, so pacing
//! vends paces both endpoints at the ceiling. The two Iceberg routes are left
//! alone: the user asked for the Unity endpoints, and an Iceberg walk already
//! paces its catalog levels.

use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};

/// The catalog's per-endpoint ceiling: the daemon's only pacing rate.
const VENDS_PER_SECOND: u32 = 15;

/// How long a `429` holds the pacer when its answer names no `Retry-After`.
const DEFAULT_BACKOFF: Duration = Duration::from_secs(1);

/// The longest a `429` can hold the pacer, so a hostile or broken header
/// cannot stall a walk for minutes.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// A strict-interval limiter: one vend every 1/15 s, no burst.
pub(crate) struct UnityRate {
    interval: Duration,
    next: Mutex<Instant>,
}

impl Default for UnityRate {
    fn default() -> Self {
        Self::new()
    }
}

impl UnityRate {
    pub(crate) fn new() -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / f64::from(VENDS_PER_SECOND)),
            next: Mutex::new(Instant::now()),
        }
    }

    /// Wait for this vend's turn. Waiters queue in arrival order; the mutex is
    /// held across the sleep so two vends never start together.
    pub(crate) async fn wait(&self) {
        let mut next = self.next.lock().await;
        let now = Instant::now();
        if *next > now {
            sleep(*next - now).await;
        }
        *next = Instant::now().max(*next) + self.interval;
    }

    /// Hold every vend for a catalog that answered HTTP `429`: its
    /// `Retry-After`, or one second when it named none, capped at a minute.
    /// Pushing the shared `next` forward backs off every in-flight walk, not
    /// just the request that was throttled.
    pub(crate) async fn backoff(&self, retry_delay: Option<Duration>) {
        let hold = retry_delay.unwrap_or(DEFAULT_BACKOFF).min(MAX_BACKOFF);
        let mut next = self.next.lock().await;
        let until = Instant::now() + hold;
        if until > *next {
            *next = until;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn vends_are_spaced() {
        let rate = UnityRate::new();
        let started = Instant::now();
        rate.wait().await;
        rate.wait().await;
        rate.wait().await;
        assert!(
            started.elapsed() >= Duration::from_millis(120),
            "three vends were not paced at 15/s: {:?}",
            started.elapsed()
        );
    }

    /// A `429` that names no `Retry-After` holds the next vend for the default
    /// second rather than the usual 1/15 s.
    #[tokio::test]
    async fn a_429_without_a_retry_header_holds_a_second() {
        let rate = UnityRate::new();
        rate.backoff(None).await;
        let started = Instant::now();
        rate.wait().await;
        assert!(
            started.elapsed() >= Duration::from_millis(900),
            "the throttled vend waited only {:?}",
            started.elapsed()
        );
    }

    /// A `Retry-After` that is shorter than the default still holds the
    /// catalog's own value, and one past the ceiling is capped at a minute.
    #[tokio::test]
    async fn a_retry_header_is_honoured_within_the_ceiling() {
        let rate = UnityRate::new();
        rate.backoff(Some(Duration::from_millis(200))).await;
        let started = Instant::now();
        rate.wait().await;
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(180), "{waited:?}");
        assert!(
            waited < Duration::from_secs(1),
            "a 200 ms Retry-After was not honoured: {waited:?}"
        );

        let capped = UnityRate::new();
        capped.backoff(Some(Duration::from_secs(600))).await;
        let until = *capped.next.lock().await;
        let hold = until.saturating_duration_since(Instant::now());
        assert!(hold <= Duration::from_secs(60), "the backoff was {hold:?}");
        assert!(hold >= Duration::from_secs(59), "the backoff was {hold:?}");
    }
}
