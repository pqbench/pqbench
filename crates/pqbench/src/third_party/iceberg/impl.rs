//! Iceberg snapshot resolution, isolated behind the `iceberg` feature.
//!
//! [`load`] is the runtime dispatch: without the feature it returns an error
//! naming it, so callers never see a `#[cfg]`.

use crate::table::{LoadRequest, TableInfo};
use crate::Lake;

#[cfg(feature = "iceberg")]
mod load;

pub(crate) async fn load(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<TableInfo, crate::table::Error> {
    #[cfg(feature = "iceberg")]
    {
        load::load(lake, request)
            .await
            .map_err(|error| crate::table::Error(error.to_string()))
    }
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error(
            "iceberg tables require the `iceberg` feature (`iceberg-s3` for S3)".into(),
        ))
    }
}
