//! Delta snapshot resolution, isolated behind the `delta` feature.
//!
//! [`load`] and [`stream_commits`] are the runtime dispatch: without the
//! feature they return an error naming it, so callers never see a `#[cfg]`.

use crate::table::{LoadRequest, TableInfo};
use crate::Lake;

#[cfg(feature = "delta")]
mod load;

pub(crate) async fn load(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<TableInfo, crate::table::Error> {
    #[cfg(feature = "delta")]
    {
        load::load(lake, request)
            .await
            .map_err(|error| crate::table::Error(error.to_string()))
    }
    #[cfg(not(feature = "delta"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error(
            "delta tables require the `delta` feature (`delta-s3` for S3)".into(),
        ))
    }
}

pub(crate) async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::CommitStream<'static>, crate::table::Error> {
    #[cfg(feature = "delta")]
    {
        use futures_util::{StreamExt as _, TryStreamExt as _};

        let commits = load::stream_commits(lake, request)
            .await
            .map_err(|error| crate::table::Error(error.to_string()))?;
        Ok(commits
            .map_err(|error| crate::table::Error(error.to_string()))
            .boxed())
    }
    #[cfg(not(feature = "delta"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error(
            "delta tables require the `delta` feature (`delta-s3` for S3)".into(),
        ))
    }
}
