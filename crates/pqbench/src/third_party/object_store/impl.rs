//! Object storage backends, isolated behind the `aws` feature.
//!
//! [`Stores`] is the S3 store registry the shared [`crate::Context`] owns. The
//! runtime dispatch is its methods: without the feature each returns an error
//! naming it, so callers never see a `#[cfg]`.

#[cfg(not(feature = "aws"))]
use url::Url;

#[cfg(not(feature = "aws"))]
use super::api::{Error, ObjectReader, PrefixListing};

#[cfg(not(feature = "aws"))]
use crate::third_party::reqwest::r#impl::Http;

#[cfg(feature = "aws")]
mod remote;

#[cfg(feature = "aws")]
pub(crate) use remote::Stores;

/// Without the `aws` feature there is no S3 backend; the handle still exists so
/// [`crate::Context`] is uniform, and every method reports the missing feature.
#[cfg(not(feature = "aws"))]
#[derive(Clone)]
pub(crate) struct Stores;

#[cfg(not(feature = "aws"))]
impl Stores {
    pub(crate) fn new(_http: &Http) -> Self {
        Self
    }

    pub(crate) fn open_remote(
        &self,
        _url: &Url,
        _options: &[(String, String)],
    ) -> Result<ObjectReader, Error> {
        Err(Error(
            "object URI scheme `s3` requires the `aws` feature".into(),
        ))
    }

    pub(crate) async fn list_remote(
        &self,
        _url: &Url,
        _options: &[(String, String)],
    ) -> Result<PrefixListing, Error> {
        Err(Error(
            "object URI scheme `s3` requires the `aws` feature".into(),
        ))
    }

    pub(crate) async fn expand_glob(
        &self,
        _uri: &str,
        _options: &[(String, String)],
    ) -> Result<Vec<String>, Error> {
        Err(Error("S3 glob expansion requires the `aws` feature".into()))
    }
}

/// Render an error with its whole source chain. `Display` alone drops the
/// cause: an object-store transport failure reads only "error sending
/// request", while the socket-level error (reset, exhaustion, DNS) sits one
/// `source()` deeper.
#[cfg(any(feature = "aws", feature = "delta"))]
pub(crate) fn error_chain(error: &dyn std::error::Error) -> String {
    let mut rendered = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        rendered.push_str("\n  caused by: ");
        rendered.push_str(&error.to_string());
        source = error.source();
    }
    rendered
}
