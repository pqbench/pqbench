//! The Unity Catalog client, isolated behind the `unity` feature.
//!
//! [`list_tables`] is the runtime dispatch: without the feature it returns an
//! error naming it, so callers never see a `#[cfg]`.

use super::api::{Error, LakeSource, NameFilter};
use crate::lake::LakeTable;

#[cfg(feature = "unity")]
mod client;
#[cfg(feature = "unity")]
mod filter;

pub(crate) async fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
) -> Result<Vec<LakeTable>, Error> {
    #[cfg(feature = "unity")]
    {
        client::list_tables(source, filter).await
    }
    #[cfg(not(feature = "unity"))]
    {
        let _ = (source, filter);
        Err(Error::from(
            "this build lists directories only; rebuild with --features unity for a Unity Catalog"
                .to_string(),
        ))
    }
}
