//! Delta snapshot resolution, isolated behind the `delta` feature.
//!
//! [`load`] is the runtime dispatch: without the feature it returns an error
//! naming it, so callers never see a `#[cfg]`.

use crate::table::{LoadRequest, TableInfo};

#[cfg(feature = "delta")]
mod load;

pub(crate) async fn load(request: &LoadRequest) -> Result<TableInfo, crate::table::Error> {
    #[cfg(feature = "delta")]
    {
        load::load(request)
            .await
            .map_err(|error| crate::table::Error(error.to_string()))
    }
    #[cfg(not(feature = "delta"))]
    {
        let _ = request;
        Err(crate::table::Error(
            "delta tables require the `delta` feature (`delta-s3` for S3)".into(),
        ))
    }
}
