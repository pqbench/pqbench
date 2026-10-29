//! The capabilities a command runs under, split by what the command uses.
//!
//! The metadata commands talk to a catalog through [`Workspace`] (the resolved
//! endpoint, bearer, and dialect, plus the pooled HTTP client); the file-reading
//! commands open a lake through [`Lake`] (the S3 store registry, built over the
//! same client). Both are built once at startup — by the CLI and the Python
//! module — and each command takes only the capability it uses, so no command
//! depends on another's needs and no library code reaches for a process global.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::config::Config;
use crate::ratelimit::RateLimit;
use crate::third_party::object_store::r#impl::Stores;
use crate::third_party::reqwest::r#impl::Http;
use crate::third_party::reqwest::{Request, Response};

/// The metadata capability: the catalog identity, the one pooled client, and
/// the one pacer every catalog request waits on. Cloning is cheap — the client
/// and the pacer are `Arc`s inside.
#[derive(Clone)]
pub struct Workspace {
    config: Config,
    http: Http,
    limit: Arc<Mutex<RateLimit>>,
}

impl Workspace {
    /// Build the pooled HTTP client over the resolved configuration.
    ///
    /// # Errors
    /// Fails when the operating system's TLS trust store cannot be read.
    pub fn new(config: Config) -> Result<Self, crate::third_party::reqwest::Error> {
        let http = Http::new(config.request_timeout)?;
        let limit = Arc::new(Mutex::new(RateLimit::new(
            config.catalog_requests_per_second,
        )));
        Ok(Self {
            config,
            http,
            limit,
        })
    }

    /// The resolved configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// This workspace over the same client, paced at `catalog_requests_per_second`.
    // aipnaming: allow(aip-136/method-prepositions)
    #[must_use]
    pub fn with_catalog_requests_per_second(&self, catalog_requests_per_second: f64) -> Self {
        let mut config = self.config.clone();
        config.catalog_requests_per_second = catalog_requests_per_second;
        Self {
            config,
            http: self.http.clone(),
            limit: Arc::new(Mutex::new(RateLimit::new(catalog_requests_per_second))),
        }
    }

    pub(crate) fn http(&self) -> &Http {
        &self.http
    }

    /// Send one catalog request after the pacer's slot. Storage reads call
    /// [`Self::http`] directly and are not paced.
    pub(crate) async fn request(
        &self,
        request: Request,
    ) -> Result<Response, crate::third_party::reqwest::Error> {
        let delay = self
            .limit
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .delay(Instant::now());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        self.http.request(request).await
    }
}

/// The storage capability: the S3 store registry, sharing the workspace's
/// client so the S3 connector reuses its one connection pool. Cloning is cheap
/// — the registry is an `Arc` inside.
#[derive(Clone)]
pub struct Lake {
    stores: Stores,
}

impl Lake {
    /// Build the store registry over the shared client.
    pub fn new(workspace: &Workspace) -> Self {
        Self {
            stores: Stores::new(workspace.http()),
        }
    }

    pub(crate) fn stores(&self) -> &Stores {
        &self.stores
    }
}
