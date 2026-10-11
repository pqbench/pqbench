//! The shared per-run context: one typed [`Config`] and the resource instances
//! the third-party wrappers return. Built once at startup and passed down as a
//! single argument, so no library code reaches for a process-global singleton.

use crate::config::Config;
use crate::third_party::object_store::r#impl::Stores;
use crate::third_party::reqwest::r#impl::Http;

/// Everything a command shares: the resolved configuration and the pooled
/// resources built over it. Cloning is cheap — the resources are `Arc`s inside.
#[derive(Clone)]
pub struct Context {
    config: Config,
    http: Http,
    stores: Stores,
}

impl Context {
    /// Build the shared HTTP client and the S3 store registry over it, plus the
    /// resolved configuration. Called once, by the CLI (and the Python module).
    ///
    /// # Errors
    /// Fails when the operating system's TLS trust store cannot be read.
    pub fn new(config: Config) -> Result<Self, crate::third_party::reqwest::Error> {
        let http = Http::new()?;
        let stores = Stores::new(&http);
        Ok(Self {
            config,
            http,
            stores,
        })
    }

    /// The resolved configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn http(&self) -> &Http {
        &self.http
    }

    pub(crate) fn stores(&self) -> &Stores {
        &self.stores
    }
}
