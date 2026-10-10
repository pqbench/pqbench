//! The `partition ls` internals: read the log, keep the named commits' adds.

use std::collections::{BTreeMap, BTreeSet};

use crate::table::{self, LoadRequest, TableFile};

use super::api::Error;

pub(crate) async fn list(
    uri: &str,
    env: &BTreeMap<String, String>,
    versions: &[u64],
) -> Result<Vec<TableFile>, Error> {
    let wanted: BTreeSet<u64> = versions.iter().copied().collect();
    let request = LoadRequest::new(uri.to_string(), None, env.clone()).with_log();
    let info = table::load(&request)
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    let mut files = Vec::new();
    for commit in &info.log {
        if !wanted.contains(&commit.version) {
            continue;
        }
        for action in &commit.actions {
            if action.kind != "add" {
                continue;
            }
            if let Some(path) = &action.path {
                let file_uri =
                    table::join_uri(uri, path).map_err(|error| Error::from(error.to_string()))?;
                files.push(TableFile::new(path.clone(), file_uri, 0));
            }
        }
    }
    Ok(files)
}
