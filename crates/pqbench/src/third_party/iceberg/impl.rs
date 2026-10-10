//! Iceberg snapshot resolution, isolated behind the `iceberg` feature.
//!
//! [`load`] is the runtime dispatch: without the feature it returns an error
//! naming it, so callers never see a `#[cfg]`.

use crate::table::{LoadRequest, TableInfo};

#[cfg(feature = "iceberg")]
mod load;

pub(crate) async fn load(request: &LoadRequest) -> Result<TableInfo, crate::table::Error> {
    #[cfg(feature = "iceberg")]
    {
        load::load(request)
            .await
            .map_err(|error| crate::table::Error(error.to_string()))
    }
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = request;
        Err(crate::table::Error(
            "iceberg tables require the `iceberg` feature (`iceberg-s3` for S3)".into(),
        ))
    }
}
