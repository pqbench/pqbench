//! One line of JSON each way between a client and the master.
//!
//! The protocol is deliberately tiny: a client asks whether the master is
//! alive, tells it to stop, or reads the map (its partition versions under a
//! parent) and one version's stored results. No method names, no RPC.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A client's request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum Request {
    /// Report whether the master is alive.
    Ping,
    /// Stop the master after answering.
    Stop,
    /// The map's partition versions under `parent` (every one when it is
    /// empty): the answer's `count` says how many lines follow it, one
    /// PartitionVersion each.
    ListPartitionVersions { parent: String },
    /// One version's stored results under `name` (a version, or a partition
    /// for its newest live version), in the answer's `records`.
    PartitionResults { name: String },
}

impl Request {
    pub fn ping() -> Self {
        Self::Ping
    }

    pub fn stop() -> Self {
        Self::Stop
    }

    pub fn list_partition_versions(parent: impl Into<String>) -> Self {
        Self::ListPartitionVersions {
            parent: parent.into(),
        }
    }

    pub fn partition_results(name: impl Into<String>) -> Self {
        Self::PartitionResults { name: name.into() }
    }
}

/// The master's answer.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Response {
    /// The answer to a ping or a stop.
    #[serde(skip_serializing_if = "is_false")]
    pub ok: bool,
    /// The records a `partition_results` request read.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<Value>,
    /// The call failed; the reader drops the ref with this cause.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// How many version lines follow the answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
}

fn is_false(value: &bool) -> bool {
    !*value
}
