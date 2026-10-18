//! The `file::list` internals: stream the files a partition's commits added.

use std::collections::BTreeMap;

use futures_util::{StreamExt, TryStreamExt};

use crate::table::{self, LoadRequest};
use crate::v1;
use crate::Lake;

use super::api::{Error, FileStream};

pub(crate) async fn list(
    lake: &Lake,
    table: &str,
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
    // The log names each file by its path; name it under the caller's table,
    // which a catalog named.
    let table = table.to_string();
    Ok(files
        .map_ok(move |mut file| {
            file.name = v1::name::table_file_name(&table, &file.path);
            file
        })
        .map_err(|error| Error::from(error.to_string()))
        .boxed())
}
