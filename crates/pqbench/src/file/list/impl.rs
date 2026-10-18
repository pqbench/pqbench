//! The `file::list` internals: stream the files a partition's commits added.

use std::collections::BTreeMap;

use futures_util::{StreamExt, TryStreamExt};

use crate::table::{self, LoadRequest};
use crate::Lake;

use super::api::{Error, FileStream};

pub(crate) async fn list(
    lake: &Lake,
    uri: &str,
    env: &BTreeMap<String, String>,
    versions: &[u64],
) -> Result<FileStream<'static>, Error> {
    // The files a commit added ride on the log itself, so the read names the
    // commits and takes their own `add` actions: no snapshot, no checkpoint, no
    // active-file replay. Naming them also keeps the read to the window.
    let request =
        LoadRequest::new(uri.to_string(), None, env.clone()).with_commits(versions.iter().copied());
    let files = table::stream_files(lake, &request)
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    Ok(files
        .map_err(|error| Error::from(error.to_string()))
        .boxed())
}
