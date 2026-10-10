//! The `table ls` internals: load the commit log, group by commit time.

use std::collections::BTreeMap;

use crate::table::{self, LoadRequest};

use super::api::{Commit, Error, Partition};

pub(crate) async fn list(
    uri: &str,
    env: &BTreeMap<String, String>,
    window_millis: i64,
) -> Result<Vec<Partition>, Error> {
    if window_millis <= 0 {
        return Err(Error::from("partition window must be positive".to_string()));
    }
    let request = LoadRequest::new(uri.to_string(), None, env.clone()).with_log();
    let info = table::load(&request)
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    Ok(group(&info.log, window_millis))
}

/// Bucket commits into epoch-aligned half-open windows. Commits keep log order.
fn group(log: &[table::LogCommit], window_millis: i64) -> Vec<Partition> {
    let mut groups: BTreeMap<i64, Vec<Commit>> = BTreeMap::new();
    for commit in log {
        let Some(commit_time) = commit.commit_time else {
            continue;
        };
        let first_time = commit_time.div_euclid(window_millis) * window_millis;
        groups.entry(first_time).or_default().push(Commit {
            version: commit.version,
            commit_time,
        });
    }
    groups
        .into_iter()
        .map(|(first_time, commits)| Partition {
            first_time,
            last_time: first_time + window_millis,
            commits,
        })
        .collect()
}
