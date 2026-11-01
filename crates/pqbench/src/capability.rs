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
