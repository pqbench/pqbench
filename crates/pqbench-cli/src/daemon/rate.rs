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

/// The catalog's per-endpoint ceiling, also the walk's `ratelimit` default.
const VENDS_PER_SECOND: u32 = 15;

/// A strict-interval limiter: one vend every 1/15 s, no burst.
pub(crate) struct UnityRate {
    interval: Duration,
    next: Mutex<Instant>,
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
}
