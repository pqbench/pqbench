//! The `file::list` internals: resolve the snapshot and stream its files out.

use std::collections::BTreeMap;

use crate::table::{self, LoadRequest};
use crate::time::TimeWindow;
use crate::v1;
use crate::Lake;

use super::api::Error;

pub(crate) async fn visit(
    lake: &Lake,
    table: &str,
    uri: &str,
    env: &BTreeMap<String, String>,
    lease: Option<v1::Credentials>,
    window: Option<TimeWindow>,
    visit: &mut (dyn FnMut(v1::TableFile) -> bool + Send),
) -> Result<u64, Error> {
    // The files are the snapshot's own active set, so the log is not read: the
    // record is the files, and carrying the commit history would only cost
    // memory.
    let request = LoadRequest::new(uri.to_string(), None, env.clone()).without_log();
    let snapshot_version = table::visit_active_files(lake, &request, &mut |mut file| {
        if !window.is_none_or(|window| window.contains(file.modification_time)) {
            return true;
        }
        file.name = v1::name::table_file_name(table, &file.path);
        file.credentials = lease.clone();
        visit(file)
    })
    .await
    .map_err(Error::Table)?;
    Ok(snapshot_version)
}
