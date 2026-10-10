//! The `partition ls` internals: read the log, keep the named commits' adds.

use std::collections::{BTreeMap, BTreeSet};

use crate::table::{self, LoadEvent, LoadRequest, TableFile};

use super::api::Error;

pub(crate) async fn list(
    uri: &str,
    env: &BTreeMap<String, String>,
    versions: &[u64],
) -> Result<Vec<TableFile>, Error> {
    let wanted: BTreeSet<u64> = versions.iter().copied().collect();
    let request = LoadRequest::new(uri.to_string(), None, env.clone()).with_log();
    let mut files = Vec::new();
    table::visit_load(&request, async |event| {
        let LoadEvent::COMMIT { commit } = event else {
            return Ok(());
        };
        if !wanted.contains(&commit.version) {
            return Ok(());
        }
        for action in &commit.actions {
            if action.kind != "add" {
                continue;
            }
            if let Some(path) = &action.path {
                let uri = table::join_uri(uri, path)?;
                files.push(TableFile::new(path.clone(), uri, 0));
            }
        }
        Ok(())
    })
    .await
    .map_err(|error| Error::from(error.to_string()))?;
    Ok(files)
}
