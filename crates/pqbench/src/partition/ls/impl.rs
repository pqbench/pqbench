//! The `partition::ls` internals: stream a table's commits as commit-time
//! windows.

use std::collections::BTreeMap;

use futures_util::{Stream, StreamExt, TryStreamExt};

use crate::table::{self, LoadRequest};
use crate::v1;
use crate::Lake;

use super::api::{Error, Partition, PartitionStream};

pub(crate) async fn list(
    lake: &Lake,
    table: &str,
    uri: &str,
    env: &BTreeMap<String, String>,
    window_millis: i64,
) -> Result<PartitionStream<'static>, Error> {
    if window_millis <= 0 {
        return Err(Error::from("partition window must be positive".to_string()));
    }
    let request = LoadRequest::new(uri.to_string(), None, env.clone()).without_files();
    let credentials = v1::Credentials::from_env(env);
    let credentials = (!credentials.is_empty()).then_some(credentials);
    let commits = table::stream_commits(lake, &request)
        .await
        .map_err(|error| Error::from(error.to_string()))?;
    Ok(windows(
        commits,
        window_millis,
        table.to_string(),
        uri.to_string(),
        credentials,
    )
    .boxed())
}

/// What a windowed read holds while it runs: the commits still to read, and the
/// window being filled. A window is handed over as soon as a commit falls outside
/// it, so nothing accumulates but that one window.
struct Windows {
    commits: table::CommitStream<'static>,
    window_millis: i64,
    table: String,
    uri: String,
    credentials: Option<v1::Credentials>,
    open: Option<(i64, Vec<u64>)>,
}

/// Stream the commits as partitions, one per window, each yielded as it closes.
fn windows(
    commits: table::CommitStream<'static>,
    window_millis: i64,
    table: String,
    uri: String,
    credentials: Option<v1::Credentials>,
) -> impl Stream<Item = Result<Partition, Error>> {
    let state = Windows {
        commits,
        window_millis,
        table,
        uri,
        credentials,
        open: None,
    };
    futures_util::stream::try_unfold(state, Windows::next)
}

impl Windows {
    /// The next partition: the open window once a commit falls outside it, or the
    /// last window when the commits end.
    async fn next(mut self) -> Result<Option<(Partition, Self)>, Error> {
        while let Some(commit) = self
            .commits
            .try_next()
            .await
            .map_err(|error| Error::from(error.to_string()))?
        {
            let Some(commit_time) = commit.commit_time else {
                continue;
            };
            let first_time = commit_time.div_euclid(self.window_millis) * self.window_millis;
            match self.open.take() {
                Some((open, mut commit_versions)) if first_time >= open => {
                    commit_versions.push(commit.version);
                    self.open = Some((open, commit_versions));
                }
                Some((open, commit_versions)) => {
                    self.open = Some((first_time, vec![commit.version]));
                    return Ok(Some((self.partition(open, commit_versions), self)));
                }
                None => self.open = Some((first_time, vec![commit.version])),
            }
        }
        Ok(self
            .open
            .take()
            .map(|(open, versions)| (self.partition(open, versions), self)))
    }

    fn partition(&self, first_time: i64, mut commit_versions: Vec<u64>) -> Partition {
        // The walk reads newest first, so a window's versions arrive in reverse.
        commit_versions.sort_unstable();
        Partition {
            name: v1::name::partition_name(&self.table, &first_time.to_string()),
            start_time: u64::try_from(first_time).ok(),
            end_time: u64::try_from(first_time + self.window_millis).ok(),
            commit_versions,
            uri: (!self.uri.is_empty()).then(|| self.uri.clone()),
            credentials: self.credentials.clone(),
        }
    }
}
