//! The catalog client.
//!
//! [`list_tables`] probes the dialect with `GET /v1/config` (Iceberg REST or
//! Unity) and runs the matching walk.
//!
//! The backends live beside this file, so they are declared here by path.

use super::api::{Error, LakeSource, NameFilter};
use crate::lake::LakeTable;

#[path = "client.rs"]
mod client;
#[path = "filter.rs"]
mod filter;
#[path = "http.rs"]
mod http;
#[path = "iceberg_rest.rs"]
mod iceberg_rest;
#[path = "protocol.rs"]
mod protocol;

pub(crate) async fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    let token = source.token.as_deref().filter(|token| !token.is_empty());
    match protocol::select(&source.endpoint, token).await? {
        protocol::Protocol::IcebergRest => iceberg_rest::list_tables(source, filter).await,
        protocol::Protocol::Unity => client::list_tables(source, filter).await,
    }
}
