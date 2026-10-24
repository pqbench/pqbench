//! The capabilities a command runs under, split by what the command uses.
//!
//! The metadata commands talk to a catalog through [`Workspace`] (the resolved
//! endpoint, bearer, and dialect, plus the pooled HTTP client); the file-reading
//! commands open a lake through [`Lake`] (the S3 store registry, built over the
//! same client). Both are built once at startup — by the CLI and the Python
//! module — and each command takes only the capability it uses, so no command
//! depends on another's needs and no library code reaches for a process global.

use crate::config::Config;
use crate::third_party::object_store::r#impl::Stores;
use crate::third_party::reqwest::r#impl::Http;

/// The metadata capability: the catalog identity and the one pooled client.
/// Cloning is cheap — the client is an `Arc` inside.
#[derive(Clone)]
pub struct Workspace {
    config: Config,
    http: Http,
}

impl Workspace {
    /// Build the pooled HTTP client over the resolved configuration.
    ///
    /// # Errors
    /// Fails when the operating system's TLS trust store cannot be read.
    pub fn new(config: Config) -> Result<Self, crate::third_party::reqwest::Error> {
        let http = Http::new(config.request_timeout)?;
        Ok(Self { config, http })
    }

    /// The resolved configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn http(&self) -> &Http {
        &self.http
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
