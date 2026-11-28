//! The capabilities a command runs under, split by what the command uses.
//!
//! The metadata commands talk to a catalog through [`Workspace`] (the resolved
//! endpoint, bearer, and dialect, plus the pooled HTTP client); the file-reading
//! commands open a lake through [`Lake`] (the S3 store registry, built over the
//! same client). Both are built once at startup — by the CLI and the Python
//! module — and each command takes only the capability it uses, so no command
//! depends on another's needs and no library code reaches for a process global.
//!
//! A workspace may also carry an HTTP GET hook: a caller-supplied transport for
//! its catalog GETs, which is how the CLI sends every catalog read through its
//! daemon. Everything above the transport — the status policy and the JSON
//! decoding — stays the same, so a hooked workspace and a plain one answer
//! identically.

use std::sync::Arc;

use futures_util::future::{BoxFuture, FutureExt};

use crate::config::Config;
use crate::third_party::object_store::r#impl::Stores;
use crate::third_party::reqwest::r#impl::Http;
use crate::third_party::reqwest::{Error, Response};

/// The transport one hooked GET runs, and the answer it reports.
pub(crate) type HttpGet =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<Response, Error>> + Send + Sync>;

/// The metadata capability: the catalog identity and the one pooled client.
/// Cloning is cheap — the client and the hook are `Arc`s inside.
#[derive(Clone)]
pub struct Workspace {
    config: Config,
    http: Http,
    http_get: Option<HttpGet>,
}

impl Workspace {
    /// Build the pooled HTTP client over the resolved configuration.
    ///
    /// # Errors
    /// Fails when the operating system's TLS trust store cannot be read.
    pub fn new(config: Config) -> Result<Self, crate::third_party::reqwest::Error> {
        let http = Http::new(config.request_timeout)?;
        Ok(Self {
            config,
            http,
            http_get: None,
        })
    }

    /// The resolved configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// This workspace with `token` as its bearer, for a caller that minted one:
    /// the same client, the same pool, a configuration that carries the token
    /// every catalog request reads.
    // aipnaming: allow(aip-136/method-prepositions)
    #[must_use]
    pub fn with_token(&self, token: impl Into<String>) -> Self {
        Self {
            config: Config {
                token: Some(token.into()),
                ..self.config.clone()
            },
            ..self.clone()
        }
    }

    /// This workspace with `http_get` sending its catalog GETs: the transport
    /// is called with the URL and answers as the pool would (a status, headers,
    /// and a body), so the status policy and the JSON decoding above it do not
    /// change. The hook owns its state and is called concurrently, since a walk
    /// has many requests in flight.
    // aipnaming: allow(aip-136/method-prepositions)
    #[must_use]
    pub fn with_http_get<F, Fut>(&self, http_get: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Response, Error>> + Send + 'static,
    {
        let http_get: HttpGet = Arc::new(move |url| http_get(url).boxed());
        Self {
            http_get: Some(http_get),
            ..self.clone()
        }
    }

    pub(crate) fn http(&self) -> &Http {
        &self.http
    }

    /// The hook this workspace's catalog GETs run, when one is attached.
    pub(crate) fn http_get(&self) -> Option<&HttpGet> {
        self.http_get.as_ref()
    }
}

/// The storage capability: the S3 store registry, sharing the workspace's
/// client so the S3 connector reuses its one connection pool, plus the storage
/// options a read uses. Cloning is cheap — the registry is an `Arc` inside.
#[derive(Clone)]
pub struct Lake {
    stores: Stores,
    options: Vec<(String, String)>,
}

impl Lake {
    /// Build the store registry over the shared client.
    pub fn new(workspace: &Workspace) -> Self {
        Self {
            stores: Stores::new(workspace.http()),
            options: Vec::new(),
        }
    }

    /// A view of this lake that reads with `lease`.
    ///
    /// The registry is shared, so a lease costs one `Arc` clone; each distinct
    /// lease still builds its own store, keyed by its options. A missing (or
    /// empty) lease is the operator's own environment: the store builder reads
    /// the process `AWS_*` variables, which is how local files and public
    /// buckets read. Pass a lease to read as the vended identity.
    // aipnaming: allow(aip-136/method-prepositions)
    #[must_use]
    pub fn with_lease(&self, lease: Option<&crate::v1::Credentials>) -> Self {
        let mut options: Vec<(String, String)> = lease
            .map(crate::v1::Credentials::to_env)
            .unwrap_or_default()
            .into_iter()
            .collect();
        options.sort();
        Self {
            stores: self.stores.clone(),
            options,
        }
    }

    pub(crate) fn stores(&self) -> &Stores {
        &self.stores
    }

    /// The storage options every read under this view uses.
    pub(crate) fn options(&self) -> &[(String, String)] {
        &self.options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lake() -> Lake {
        Lake::new(&Workspace::new(Default::default()).expect("a workspace"))
    }

    #[test]
    fn with_lease_sorts_the_options_and_leaves_the_parent_alone() {
        let lake = lake();
        let lease = crate::v1::Credentials {
            access_key_id: Some("id".to_string()),
            secret_access_key: Some("secret".to_string()),
            ..crate::v1::Credentials::default()
        };
        let scoped = lake.with_lease(Some(&lease));
        assert_eq!(
            scoped.options(),
            [
                ("AWS_ACCESS_KEY_ID".to_string(), "id".to_string()),
                ("AWS_SECRET_ACCESS_KEY".to_string(), "secret".to_string()),
            ]
        );
        assert!(lake.options().is_empty());
    }

    #[test]
    fn no_lease_is_the_operator_environment() {
        let lake = lake();
        assert!(lake.with_lease(None).options().is_empty());
        assert!(lake
            .with_lease(Some(&crate::v1::Credentials::default()))
            .options()
            .is_empty());
    }
}
