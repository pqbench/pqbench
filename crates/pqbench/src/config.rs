//! The configuration a walk command runs under, and the layered resolution that
//! produces it.
//!
//! One typed [`Config`] holds every setting. It is built from a stack of
//! [`Layer`]s, least to most specific — a run-control file, then the
//! environment, then the command line — combined with [`Combine`]; the CLI
//! builds the layers, the library owns the type.

use std::time::Duration;

/// Catalog requests per second a command paces at by default: the Databricks
/// SDKs' own ceiling, and well under the rate the endpoint starts refusing.
pub const DEFAULT_REQUESTS_PER_SECOND: f64 = 15.0;

/// The catalog dialect a command speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// Unity REST: `/schemas`, `/tables`.
    Unity,
    /// Iceberg REST: `/namespaces…`; the endpoint already names the catalog
    /// base (`{root}/v1` or `{root}/v1/{prefix}`), so no config probe runs.
    Iceberg,
}

/// One command's resolved configuration: the endpoint and bearer a metadata
/// command runs under, and the catalog dialect it speaks.
#[derive(Debug, Clone)]
pub struct Config {
    /// The endpoint root. Empty when a command reads storage by path only.
    pub endpoint: String,
    /// The bearer credential, when the endpoint needs one.
    pub token: Option<String>,
    /// The catalog dialect.
    pub dialect: Dialect,
    /// How long one request may take. `None` leaves it to the HTTP client's own
    /// default; a front end that knows the store's latency sets it.
    pub request_timeout: Option<Duration>,
    /// Catalog requests per second. `0` turns pacing off; the pacer covers
    /// catalog calls only, never object storage.
    pub requests_per_second: f64,
}

impl Default for Config {
    /// No endpoint, no token, the default dialect: what a caller that reads
    /// only local storage needs.
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            token: None,
            dialect: Dialect::Unity,
            request_timeout: None,
            requests_per_second: DEFAULT_REQUESTS_PER_SECOND,
        }
    }
}

/// One source's contribution to the configuration. Every setting is optional:
/// a layer supplies only what it knows, and [`Layer::resolve`] fills the rest.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Layer {
    /// The endpoint root, when this layer names one.
    pub endpoint: Option<String>,
    /// The bearer credential, when this layer carries one.
    pub token: Option<String>,
    /// The catalog dialect, when this layer names one.
    pub dialect: Option<Dialect>,
    /// Catalog requests per second, when this layer names one.
    pub requests_per_second: Option<f64>,
}

/// A setting several layers each contribute. `self.combine(higher)` overlays a
/// more specific `higher` layer, so `higher` wins where both are set.
pub trait Combine: Sized {
    /// Overlay `higher` (a more specific layer) onto `self`.
    fn combine(self, higher: Self) -> Self;
}

impl<T> Combine for Option<T> {
    fn combine(self, higher: Self) -> Self {
        higher.or(self)
    }
}

impl Combine for Layer {
    fn combine(self, higher: Self) -> Self {
        Self {
            endpoint: self.endpoint.combine(higher.endpoint),
            token: self.token.combine(higher.token),
            dialect: self.dialect.combine(higher.dialect),
            requests_per_second: self.requests_per_second.combine(higher.requests_per_second),
        }
    }
}

impl Layer {
    /// Apply the defaults, yielding the run's [`Config`]: the dialect defaults
    /// to Unity, the pace to [`DEFAULT_REQUESTS_PER_SECOND`], and a missing
    /// endpoint stays empty — a command that reads only storage needs none.
    pub fn resolve(self) -> Config {
        Config {
            endpoint: self.endpoint.unwrap_or_default(),
            token: self.token,
            dialect: self.dialect.unwrap_or(Dialect::Unity),
            request_timeout: None,
            requests_per_second: self
                .requests_per_second
                .unwrap_or(DEFAULT_REQUESTS_PER_SECOND),
        }
    }
}
