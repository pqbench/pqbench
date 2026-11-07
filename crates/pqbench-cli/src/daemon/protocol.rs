//! One line of JSON each way between a reader and the daemon.
//!
//! The protocol is deliberately tiny: the daemon owns every catalog request,
//! so a reader either asks for a table's lease or for the catalog records a
//! command would have fetched itself. The daemon answers with the `AWS_*`
//! options and their expiry, with the catalog's records, or with the error the
//! catalog gave. No version, no method names, no RPC.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    /// A `list` level: its collection and the parent record it descends from.
    List {
        collection: Collection,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent: Option<Value>,
    },
    /// a `get` level: its resource and the parent record it enriches.
    Get {
        resource: Get,
        #[serde(skip_serializing_if = "Option::is_none")]
        record: Option<Value>,
    },
}

/// The collection a `list` request enumerates.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Collection {
    Catalogs,
    Schemas,
    Tables,
}

/// The resource a `get` request reads.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Get {
    Metastore,
    Catalog,
    Schema,
    Table,
}

impl Request {
    pub(crate) fn table(table: impl Into<String>) -> Self {
        Self::Lease {
            table: table.into(),
        }
    }

    pub(crate) fn ping() -> Self {
        Self::Ping
    }

    pub(crate) fn stop() -> Self {
        Self::Stop
    }

    pub(crate) fn catalogs() -> Self {
        Self::List {
            collection: Collection::Catalogs,
            parent: None,
        }
    }

    pub(crate) fn schemas(parent: Value) -> Self {
        Self::List {
            collection: Collection::Schemas,
            parent: Some(parent),
        }
    }

    pub(crate) fn tables(parent: Value) -> Self {
        Self::List {
            collection: Collection::Tables,
            parent: Some(parent),
        }
    }

    pub(crate) fn metastore() -> Self {
        Self::Get {
            resource: Get::Metastore,
            record: None,
        }
    }

    pub(crate) fn catalog(record: Value) -> Self {
        Self::Get {
            resource: Get::Catalog,
            record: Some(record),
        }
    }

    pub(crate) fn schema(record: Value) -> Self {
        Self::Get {
            resource: Get::Schema,
            record: Some(record),
        }
    }

    pub(crate) fn table_get(record: Value) -> Self {
        Self::Get {
            resource: Get::Table,
            record: Some(record),
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
    /// The records a `list` or `get` request read.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub records: Vec<Value>,
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
