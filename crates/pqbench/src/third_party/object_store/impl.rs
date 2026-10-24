//! Object storage backends, isolated behind the `aws` feature.
//!
//! [`Stores`] is the S3 store registry the shared [`crate::Lake`] owns. The
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
/// [`crate::Lake`] is uniform, and every method reports the missing feature.
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
        Err(Error::Unsupported(
            "object URI scheme `s3` requires the `aws` feature".into(),
        ))
    }

    pub(crate) async fn list_remote(
        &self,
        _url: &Url,
        _options: &[(String, String)],
    ) -> Result<PrefixListing, Error> {
        Err(Error::Unsupported(
            "object URI scheme `s3` requires the `aws` feature".into(),
        ))
    }

    pub(crate) async fn expand_glob(
        &self,
        _uri: &str,
        _options: &[(String, String)],
    ) -> Result<Vec<String>, Error> {
        Err(Error::Unsupported(
            "S3 glob expansion requires the `aws` feature".into(),
        ))
    }
}
