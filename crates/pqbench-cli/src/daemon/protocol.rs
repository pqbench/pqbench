//! One line of JSON each way between a reader and the lease daemon.
//!
//! The protocol is deliberately tiny: a table name, a ping, or a stop. The
//! daemon answers with the `AWS_*` options and their expiry, or with the error
//! the catalog gave. No version, no method names, no RPC.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A client's request.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct Request {
    /// The table whose lease the client needs.
    pub table: Option<String>,
    /// Report whether the daemon is alive.
    pub ping: bool,
    /// Stop the daemon after answering.
    pub stop: bool,
}

impl Request {
    pub(crate) fn table(table: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            ..Self::default()
        }
    }

    pub(crate) fn ping() -> Self {
        Self {
            ping: true,
            ..Self::default()
        }
    }

    pub(crate) fn stop() -> Self {
        Self {
            stop: true,
            ..Self::default()
        }
    }
}

/// The daemon's answer.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct Response {
    /// The `AWS_*` options, when the catalog vended a lease.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
    /// When the lease expires, milliseconds since the Unix epoch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expire_time_ms: Option<u64>,
    /// The catalog vended nothing; the reader keeps its ambient `AWS_*`.
    #[serde(skip_serializing_if = "is_false")]
    pub none: bool,
    /// The vending call failed; the reader drops the ref with this cause.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The answer to a ping or a stop.
    #[serde(skip_serializing_if = "is_false")]
    pub ok: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}
