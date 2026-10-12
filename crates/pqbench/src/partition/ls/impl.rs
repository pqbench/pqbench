//! The `partition::ls` internals: load the commit log, group by commit time.

use std::collections::BTreeMap;

use crate::table::{self, LoadRequest};
use crate::v1;
use crate::Lake;

use super::api::{Error, Partition};

pub(crate) async fn list(
    lake: &Lake,
    table: &str,
    uri: &str,
    env: &BTreeMap<String, String>,
    window_millis: i64,
) -> Result<Vec<Partition>, Error> {
    if window_millis <= 0 {
        return Err(Error::from("partition window must be positive".to_string()));
    }
    let request = LoadRequest::new(uri.to_string(), None, env.clone()).with_log();
    let info = table::load(lake, &request)
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    let credentials = v1::Credentials::from_env(env);
    Ok(group(
        &info.log,
        table,
        uri,
        (!credentials.is_empty()).then_some(credentials),
        window_millis,
    ))
}

/// Bucket commits into epoch-aligned half-open windows. Commits keep log order.
fn group(
    log: &[table::LogCommit],
    table_name: &str,
    uri: &str,
    credentials: Option<v1::Credentials>,
    window_millis: i64,
) -> Vec<Partition> {
    let mut groups: BTreeMap<i64, Vec<u64>> = BTreeMap::new();
    for commit in log {
        let Some(commit_time) = commit.commit_time else {
            continue;
        };
        let first_time = commit_time.div_euclid(window_millis) * window_millis;
        groups.entry(first_time).or_default().push(commit.version);
    }
    groups
        .into_iter()
        .map(|(first_time, commit_versions)| Partition {
            name: v1::name::partition_name(table_name, &first_time.to_string()),
            start_time: u64::try_from(first_time).ok(),
            end_time: u64::try_from(first_time + window_millis).ok(),
            commit_versions,
            uri: (!uri.is_empty()).then(|| uri.to_string()),
            credentials: credentials.clone(),
        })
        .collect()
}
