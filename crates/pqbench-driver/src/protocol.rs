//! One line of JSON each way between a client and the driver.
//!
//! The protocol is deliberately tiny: a client asks whether the driver is
//! alive, tells it to stop, reads the map (its partition versions under a
//! parent) or one version's stored results, reads the map's state as a whole,
//! or reads the suggestions the driver measured. No method names, no RPC.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A client's request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum Request {
    /// Report whether the driver is alive.
    Ping,
    /// Stop the driver after answering.
    Stop,
    /// The map's partition versions under `parent` (every one when it is
    /// empty): the answer's `count` says how many lines follow it, one
    /// PartitionVersion each.
    ListPartitionVersions { parent: String },
    /// One version's stored results under `name` (a version, or a partition
    /// for its newest live version), in the answer's `records`.
    PartitionResults { name: String },
    /// The map as a whole, in the answer's `status`.
    Status,
    /// The suggestions the driver measured for the live versions under
    /// `parent` (every one when it is absent), in the answer's `records`,
    /// largest saving first.
    ListSuggestions { parent: Option<String> },
}

impl Request {
    pub fn ping() -> Self {
        Self::Ping
    }

    pub fn stop() -> Self {
        Self::Stop
    }

    pub fn status() -> Self {
        Self::Status
    }

    pub fn list_partition_versions(parent: impl Into<String>) -> Self {
        Self::ListPartitionVersions {
            parent: parent.into(),
        }
    }

    pub fn partition_results(name: impl Into<String>) -> Self {
        Self::PartitionResults { name: name.into() }
    }

    pub fn list_suggestions(parent: Option<String>) -> Self {
        Self::ListSuggestions { parent }
    }
}

/// The driver's answer.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Response {
    /// The answer to a ping or a stop.
    #[serde(skip_serializing_if = "is_false")]
    pub ok: bool,
    /// The records a `partition_results` or `list_suggestions` request read.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<Value>,
    /// The call failed; the reader drops the ref with this cause.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// How many version lines follow the answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    /// The map as a `status` request found it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<DriverStatus>,
}

/// The map as a whole: what the driver holds, what it is doing, and how its
/// walks have gone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DriverStatus {
    /// How many versions are in each state; every state the map can hold is
    /// named, so a reader sees a zero rather than a missing key.
    pub state_counts: BTreeMap<String, u64>,
    /// How many versions the map holds.
    pub total_versions: u64,
    /// How many worker commands the driver is running now (a version's
    /// command, or a walk).
    pub running_commands: u64,
    /// How many versions have failed their current transition and not yet
    /// moved on; after repeated failures they back off until `retry_time`.
    pub failing_versions: u64,
    /// When the last walk started, milliseconds since the Unix epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_walk_time: Option<u64>,
    /// Why the last walk failed; absent when it succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_walk_error: Option<String>,
}

fn is_false(value: &bool) -> bool {
    !*value
}
