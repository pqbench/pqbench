//! Object storage backends, isolated behind the `aws` feature.
//!
//! [`open_remote`] is the runtime dispatch: without the feature it returns an
//! error naming it, so callers never see a `#[cfg]`.

use url::Url;

use super::api::{Error, ObjectReader, PrefixListing};

#[cfg(feature = "aws")]
mod remote;

pub(crate) fn open_remote(url: &Url, options: &[(String, String)]) -> Result<ObjectReader, Error> {
    #[cfg(feature = "aws")]
    {
        remote::open_remote(url, options)
    }
    #[cfg(not(feature = "aws"))]
    {
        let _ = (url, options);
        Err(Error(
            "object URI scheme `s3` requires the `aws` feature".into(),
        ))
    }
}

pub(crate) async fn list_remote(
    url: &Url,
    options: &[(String, String)],
) -> Result<PrefixListing, Error> {
    #[cfg(feature = "aws")]
    {
        remote::list_remote(url, options).await
    }
    #[cfg(not(feature = "aws"))]
    {
        let _ = (url, options);
        Err(Error(
            "object URI scheme `s3` requires the `aws` feature".into(),
        ))
    }
}
