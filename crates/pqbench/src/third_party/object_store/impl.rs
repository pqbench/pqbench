//! Object storage backends, isolated behind the `aws` feature.
//!
//! [`open_remote`] is the runtime dispatch: without the feature it returns an
//! error naming it, so callers never see a `#[cfg]`.

use url::Url;

use super::api::{Error, ObjectReader, PrefixListing};

#[cfg(feature = "aws")]
mod remote;
#[cfg(feature = "aws")]
pub(crate) use remote::root_store;

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

pub(crate) async fn expand_glob(
    uri: &str,
    options: &[(String, String)],
) -> Result<Vec<String>, Error> {
    #[cfg(feature = "aws")]
    {
        remote::expand_glob(uri, options).await
    }
    #[cfg(not(feature = "aws"))]
    {
        let _ = (uri, options);
        Err(Error("S3 glob expansion requires the `aws` feature".into()))
    }
}
