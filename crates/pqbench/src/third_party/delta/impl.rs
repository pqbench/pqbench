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
        load::load(lake, request).await
    }
    #[cfg(not(feature = "delta"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error::Invalid(
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
        use futures_util::StreamExt as _;
        Ok(load::stream_commits(lake, request).await?.boxed())
    }
    #[cfg(not(feature = "delta"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error::Invalid(
            "delta tables require the `delta` feature (`delta-s3` for S3)".into(),
        ))
    }
}

pub(crate) async fn stream_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::FileStream<'static>, crate::table::Error> {
    #[cfg(feature = "delta")]
    {
        use futures_util::StreamExt as _;
        Ok(load::stream_files(lake, request).await?.boxed())
    }
    #[cfg(not(feature = "delta"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error::Invalid(
            "delta tables require the `delta` feature (`delta-s3` for S3)".into(),
        ))
    }
}
