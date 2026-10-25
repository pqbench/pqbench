use serde::{Deserialize, Serialize};

use super::Resource;

use super::LogAction;

/// One commit of a table's log.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, Resource)]
#[serde(default)]
pub struct Commit {
    /// `…/tables/{table}/commits/{commit}`.
    #[resource(name = "catalogs/{catalog}/schemas/{schema}/tables/{table}/commits/{commit}")]
    pub name: String,
    /// The log version.
    pub version: u64,
    /// Commit time, milliseconds since the Unix epoch, when the log records
    /// one. Delta reads `commitInfo.timestamp`; Iceberg the snapshot time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_time: Option<i64>,
    /// The commit's `commitInfo` action as the log stores it, when it has one:
    /// the operation, its parameters and metrics, and the writer's identity.
    /// Delta reads it from the first line of the commit file; Iceberg has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_info: Option<serde_json::Value>,
    /// Actions from the commit file, in file order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<LogAction>,
}
