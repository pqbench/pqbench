//! The catalog client, isolated behind the `unity` feature.
//!
//! [`list_tables`] is the runtime dispatch: without the feature it returns an
//! error naming it, so callers never see a `#[cfg]`. With it, `GET /v1/config`
//! chooses the dialect (Iceberg REST or Unity) and the matching client runs.
//!
//! This is the only file in the wrapper that carries feature flags. The
//! backends live beside `unity.rs`, so they are declared here by path.

use super::api::{Error, LakeSource, NameFilter};
use crate::lake::LakeTable;

#[cfg(feature = "unity")]
#[path = "client.rs"]
mod client;
#[cfg(feature = "unity")]
#[path = "filter.rs"]
mod filter;
#[cfg(feature = "unity")]
#[path = "http.rs"]
mod http;
#[cfg(feature = "unity")]
#[path = "iceberg_rest.rs"]
mod iceberg_rest;
#[cfg(feature = "unity")]
#[path = "protocol.rs"]
mod protocol;

pub(crate) async fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    #[cfg(feature = "unity")]
    {
        let token = source.token.as_deref().filter(|token| !token.is_empty());
        match protocol::select(&source.endpoint, token).await? {
            protocol::Protocol::IcebergRest => iceberg_rest::list_tables(source, filter).await,
            protocol::Protocol::Unity => client::list_tables(source, filter).await,
        }
    }
    #[cfg(not(feature = "unity"))]
    {
        let _ = (source, filter);
        Err(Error::from(
            "this build lists directories only; rebuild with --features unity for a catalog"
                .to_string(),
        ))
    }
}
