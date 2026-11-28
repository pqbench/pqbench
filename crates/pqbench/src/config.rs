//! The configuration a walk command runs under, and the layered resolution that
//! produces it.
//!
//! One typed [`Config`] holds every setting. It is built from a stack of
//! [`Layer`]s, least to most specific — a run-control file, then the
//! environment, then the command line — combined with [`Combine`]; the CLI
//! builds the layers, the library owns the type.
//!
//! The bearer is either static (`token`) or minted from a service principal's
//! `client_id`/`client_secret`; a static token wins, so a workspace with both
//! configured uses the one an operator handed it.

use std::fmt;
use std::time::Duration;

/// The catalog dialect a command speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    /// Unity REST: `/schemas`, `/tables`.
    Unity,
    /// Iceberg REST: `/namespaces…`; the endpoint already names the catalog
    /// base (`{root}/v1` or `{root}/v1/{prefix}`), so no config probe runs.
    Iceberg,
}

impl Dialect {
    /// The spelling `PQB_TABLE_FORMAT` accepts: `unity` or `iceberg`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unity => "unity",
            Self::Iceberg => "iceberg",
        }
    }

    /// One page of the endpoint's first listing level: Unity REST's
    /// `/catalogs`, or the Iceberg REST base's `/namespaces`. It is the route a
    /// preflight reads to prove that an endpoint and a bearer answer together.
    #[must_use]
    pub fn listing_url(self, endpoint: &str) -> String {
        match self {
            Self::Unity => format!(
                "{}/catalogs?max_results=1",
                crate::dialect::api_root(endpoint)
            ),
            Self::Iceberg => format!("{}/namespaces", crate::dialect::iceberg_root(endpoint)),
        }
    }
}

/// A credential whose value must never be printed: `Debug` names the type and
/// nothing else, so a configuration printed for diagnosis carries no secret.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// The secret an operator supplied.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The value, for the one request that sends it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
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
    /// The service principal's client id, when the bearer is minted rather than
    /// supplied.
    pub client_id: Option<String>,
    /// The service principal's client secret.
    pub client_secret: Option<Secret>,
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
            client_id: None,
            client_secret: None,
        }
    }
}

/// One source's contribution to the configuration. Every setting is optional:
/// a layer supplies only what it knows, and [`Layer::resolve`] fills the rest.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Layer {
    /// The endpoint root, when this layer names one.
    pub endpoint: Option<String>,
    /// The bearer credential, when this layer carries one.
    pub token: Option<String>,
    /// The catalog dialect, when this layer names one.
    pub dialect: Option<Dialect>,
    /// The service principal's client id, when this layer names one.
    pub client_id: Option<String>,
    /// The service principal's client secret, when this layer carries one.
    pub client_secret: Option<Secret>,
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
            client_id: self.client_id.combine(higher.client_id),
            client_secret: self.client_secret.combine(higher.client_secret),
        }
    }
}

impl Layer {
    /// Apply the defaults, yielding the run's [`Config`]: the dialect defaults
    /// to Unity, and a missing endpoint stays empty — a command that reads only
    /// storage needs none.
    pub fn resolve(self) -> Config {
        Config {
            endpoint: self.endpoint.unwrap_or_default(),
            token: self.token,
            dialect: self.dialect.unwrap_or(Dialect::Unity),
            request_timeout: None,
            client_id: self.client_id,
            client_secret: self.client_secret,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_layer_carries_the_service_principals_credentials() {
        let config = Layer {
            token: None,
            client_id: Some("id".into()),
            client_secret: Some(Secret::new("secret")),
            ..Layer::default()
        }
        .resolve();
        assert_eq!(config.client_id.as_deref(), Some("id"));
        assert_eq!(
            config.client_secret.as_ref().map(Secret::expose),
            Some("secret")
        );
        assert!(config.token.is_none());
    }

    #[test]
    fn a_more_specific_layer_wins() {
        let profile = Layer {
            endpoint: Some("profile".into()),
            dialect: Some(Dialect::Iceberg),
            ..Layer::default()
        };
        let flags = Layer {
            token: Some("flag".into()),
            client_id: Some("flag-id".into()),
            ..Layer::default()
        };
        let config = profile.combine(flags).resolve();
        assert_eq!(config.endpoint, "profile");
        assert_eq!(config.token.as_deref(), Some("flag"));
        assert_eq!(config.client_id.as_deref(), Some("flag-id"));
        assert_eq!(config.dialect, Dialect::Iceberg);
    }

    #[test]
    fn the_listing_route_follows_the_dialect() {
        assert_eq!(
            Dialect::Unity.listing_url("https://host/"),
            "https://host/api/2.1/unity-catalog/catalogs?max_results=1"
        );
        assert_eq!(
            Dialect::Unity.listing_url("https://host/api/2.1/unity-catalog"),
            "https://host/api/2.1/unity-catalog/catalogs?max_results=1"
        );
        assert_eq!(
            Dialect::Iceberg.listing_url("https://host/iceberg-rest/v1"),
            "https://host/iceberg-rest/v1/namespaces"
        );
    }
}
