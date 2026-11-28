//! One line of JSON each way between a reader and the daemon.
//!
//! The protocol is deliberately tiny and mirrors the two things only the daemon
//! can do: it owns the vended storage leases, so a reader asks for a table's
//! lease; and it owns the catalog identity and its pacing, so a reader asks it
//! to send one HTTP GET. Everything else — the URLs, the pages, the JSON — is
//! the reader's, so no catalog verb is repeated here. The daemon answers with
//! the `AWS_*` options and their expiry, with a GET's status and body, or with
//! the error the request gave. No version, no method names, no RPC.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A client's request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub(crate) enum Request {
    /// Report whether the daemon is alive.
    Ping,
    /// Stop the daemon after answering.
    Stop,
    /// The lease of one table.
    Lease { table: String },
    /// One catalog GET, sent under the daemon's own identity.
    HttpGet { url: String },
}

impl Request {
    pub(crate) fn lease(table: &str) -> Self {
        Self::Lease {
            table: table.to_string(),
        }
    }

    pub(crate) fn http_get(url: &str) -> Self {
        Self::HttpGet {
            url: url.to_string(),
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
    /// The status a `http_get` request was answered with.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    /// The body a `http_get` request was answered with, lossily decoded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// The call failed; the reader drops the ref with this cause.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The answer to a ping or a stop.
    #[serde(skip_serializing_if = "is_false")]
    pub ok: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}
