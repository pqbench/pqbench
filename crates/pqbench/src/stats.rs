//! Timing statistics: reducers over raw per-pass samples and a composable
//! measurement monoid.
//!
//! The measurement core (`codecs::bench_pages`) only records raw per-pass
//! times; every decision about those samples lives here. Warmup and mode are
//! reducers (`samples[] => samples[]`), and [`Estimate`] is a monoid, so
//! per-chunk measurements compose into a file measurement.

use std::time::Duration;

use serde::Serialize;

/// How to reduce a sample set to the reported value. Both are "the mean over
/// some subset of samples": fastest = the mean over the single best sample,
/// mean = the mean over all of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Fastest,
    Mean,
}

/// Analytics decisions applied before summarizing: warmup discards the first
/// `warmup_iterations` samples, then `mode` selects the subset to average.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub warmup_iterations: usize,
    pub mode: Mode,
}

/// One measured quantity, composable across independent experiments.
///
/// A monoid under [`combine`](Estimate::combine): for independent sweeps the
/// total time is the sum of the means and its SE² is the sum of the SE²s
/// (independent variances add). `fastest` mode falls out naturally — a single
/// sample has SE² 0.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct Estimate {
    /// Number of samples the mean is over.
    pub n: u64,
    /// Mean time per timed pass, in ns.
    pub mean: f64,
    /// SE² of the mean (`variance / n`), in ns².
    pub se2: f64,
}

impl Estimate {
    /// The empty measurement (monoid identity).
    pub fn zero() -> Estimate {
        Estimate {
            n: 0,
            mean: 0.0,
            se2: 0.0,
        }
    }

    /// Monoid combine: means and SE²s add for independent experiments.
    pub fn combine(self, other: Estimate) -> Estimate {
        Estimate {
            n: self.n + other.n,
            mean: self.mean + other.mean,
            se2: self.se2 + other.se2,
        }
    }

    /// Standard error of the mean, in ns.
    pub fn se(&self) -> f64 {
        self.se2.sqrt()
    }

    /// MB/s over `bytes` at the mean time.
    pub fn megabytes_per_second(&self, bytes: u64) -> f64 {
        if self.mean == 0.0 {
            return 0.0;
        }
        bytes as f64 / 1_000_000.0 / (self.mean / 1_000_000_000.0)
    }

    /// SE of the MB/s figure (delta method: `speed = bytes/mean`).
    pub fn megabytes_per_second_se(&self, bytes: u64) -> f64 {
        if self.mean == 0.0 {
            return 0.0;
        }
        self.megabytes_per_second(bytes) * self.se() / self.mean
    }
}

/// Reducer: drop the first `k` samples (warmup).
pub fn warmup(k: usize) -> impl Fn(Vec<Duration>) -> Vec<Duration> {
    move |mut samples: Vec<Duration>| {
        samples.drain(..k.min(samples.len()));
        samples
    }
}

/// Reducer: keep only the fastest sample (lzbench's fastest).
pub fn fastest(mut samples: Vec<Duration>) -> Vec<Duration> {
    samples.sort();
    samples.truncate(1);
    samples
}

/// Apply the config's reducers (warmup, then mode) to a sample set.
pub fn reduce(samples: &[Duration], cfg: &Config) -> Vec<Duration> {
    let mut out = warmup(cfg.warmup_iterations)(samples.to_vec());
    if cfg.mode == Mode::Fastest {
        out = fastest(out);
    }
    out
}

/// Summarize the config-selected samples as their mean ± SE (always a mean).
pub fn measure(samples: &[Duration], cfg: &Config) -> Estimate {
    summarize(&reduce(samples, cfg))
}

/// Summarize a sample set as its mean ± SE.
pub fn summarize(samples: &[Duration]) -> Estimate {
    if samples.is_empty() {
        return Estimate::zero();
    }
    let n = samples.len() as u64;
    let ns: Vec<f64> = samples.iter().map(|t| t.as_nanos() as f64).collect();
    let mean = ns.iter().sum::<f64>() / n as f64;
    let var = ns.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1).max(1) as f64;
    Estimate {
        n,
        mean,
        se2: var / n as f64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dur(ns: u64) -> Duration {
        Duration::from_nanos(ns)
    }

    #[test]
    fn summarize_computes_mean_and_se() {
        let m = summarize(&[dur(100), dur(200), dur(300)]);
        assert_eq!(m.n, 3);
        assert_eq!(m.mean, 200.0);
        // var = ((100-200)^2 + 0 + (300-200)^2)/2 = 10000; se2 = var/3
        assert!((m.se2 - 10000.0 / 3.0).abs() < 1e-6);
        assert!((m.se() - (10_000.0_f64 / 3.0).sqrt()).abs() < 1e-6);
    }

    #[test]
    fn fastest_keeps_the_min_with_zero_se() {
        let cfg = Config {
            warmup_iterations: 0,
            mode: Mode::Fastest,
        };
        let m = measure(&[dur(300), dur(100), dur(200)], &cfg);
        assert_eq!(m.n, 1);
        assert_eq!(m.mean, 100.0);
        assert_eq!(m.se2, 0.0);
    }

    #[test]
    fn warmup_drops_the_first_samples() {
        let cfg = Config {
            warmup_iterations: 2,
            mode: Mode::Mean,
        };
        let m = measure(&[dur(1000), dur(1000), dur(100), dur(200)], &cfg);
        assert_eq!(m.n, 2);
        assert_eq!(m.mean, 150.0);
    }

    #[test]
    fn combine_is_a_monoid() {
        let a = summarize(&[dur(100), dur(300)]);
        let b = summarize(&[dur(200), dur(400)]);
        let c = a.combine(b);
        assert_eq!(c.n, a.n + b.n);
        assert_eq!(c.mean, a.mean + b.mean);
        assert_eq!(c.se2, a.se2 + b.se2);
        // identity
        assert_eq!(a.combine(Estimate::zero()), a);
        // associative
        assert_eq!(a.combine(b).combine(c), a.combine(b.combine(c)));
    }

    #[test]
    fn speed_and_standard_error_propagate() {
        let m = summarize(&[dur(1_000_000), dur(3_000_000)]);
        // mean 2ms, bytes 1MB => 500 MB/s
        assert!((m.megabytes_per_second(1_000_000) - 500.0).abs() < 1e-6);
        assert!(m.megabytes_per_second_se(1_000_000) > 0.0);
    }

    #[test]
    fn summarize_empty_is_zero_not_panic() {
        let m = summarize(&[]);
        assert_eq!(m, Estimate::zero());
    }

    #[test]
    fn measure_empty_samples_is_zero() {
        let cfg = Config {
            warmup_iterations: 0,
            mode: Mode::Mean,
        };
        let m = measure(&[], &cfg);
        assert_eq!(m.n, 0);
        assert_eq!(m.megabytes_per_second(1_000_000), 0.0);
    }

    #[test]
    fn speed_se_is_zero_for_zero_estimate() {
        assert_eq!(Estimate::zero().megabytes_per_second_se(1_000_000), 0.0);
        assert!(!Estimate::zero().megabytes_per_second(1_000_000).is_nan());
    }
}
