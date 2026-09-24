//! The catalog client, isolated behind the `unity` feature.
//!
//! [`list_tables`] is the runtime dispatch: without the feature it returns an
//! error naming it, so callers never see a `#[cfg]`. With it, `GET /v1/config`
//! chooses the dialect (Iceberg REST or Unity) and the matching client runs.

use super::api::{Error, LakeSource, NameFilter};
use crate::lake::LakeTable;

pub(crate) async fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    #[cfg(feature = "unity")]
    {
        let token = source.token.as_deref().filter(|token| !token.is_empty());
        match super::protocol::select(&source.endpoint, token).await? {
            super::protocol::Protocol::IcebergRest => {
                super::iceberg_rest::list_tables(source, filter).await
            }
            super::protocol::Protocol::Unity => super::client::list_tables(source, filter).await,
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
