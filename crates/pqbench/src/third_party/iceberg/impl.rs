//! Iceberg snapshot resolution, isolated behind the `iceberg` feature.
//!
//! [`load`] and [`stream_commits`] are the runtime dispatch: without the
//! feature they return an error naming it, so callers never see a `#[cfg]`.

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
        load::load(lake, request).await
    }
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error::Invalid(
            "iceberg tables require the `iceberg` feature (`iceberg-s3` for S3)".into(),
        ))
    }
}

pub(crate) async fn stream_commits(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::CommitStream<'static>, crate::table::Error> {
    #[cfg(feature = "iceberg")]
    {
        use futures_util::StreamExt as _;

        Ok(load::stream_commits(lake, request).await?.boxed())
    }
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error::Invalid(
            "iceberg tables require the `iceberg` feature (`iceberg-s3` for S3)".into(),
        ))
    }
}

pub(crate) async fn stream_files(
    lake: &Lake,
    request: &LoadRequest,
) -> Result<crate::table::FileStream<'static>, crate::table::Error> {
    #[cfg(feature = "iceberg")]
    {
        use futures_util::StreamExt as _;

        Ok(load::stream_files(lake, request).await?.boxed())
    }
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = (lake, request);
        Err(crate::table::Error::Invalid(
            "iceberg tables require the `iceberg` feature (`iceberg-s3` for S3)".into(),
        ))
    }
}

pub(crate) async fn visit_active_files(
    lake: &Lake,
    request: &LoadRequest,
    visit: &mut (dyn FnMut(crate::v1::TableFile) -> bool + Send),
) -> Result<u64, crate::table::Error> {
    #[cfg(feature = "iceberg")]
    {
        load::visit_active_files(lake, request, visit).await
    }
    #[cfg(not(feature = "iceberg"))]
    {
        let _ = (lake, request, visit);
        Err(crate::table::Error::Invalid(
            "iceberg tables require the `iceberg` feature (`iceberg-s3` for S3)".into(),
        ))
    }
}
