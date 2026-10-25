//! The records a level reads: the model methods the generic stream needs.
//!
//! Each model derives `from_json` and `name` as inherent methods. A generic
//! stream cannot call an inherent method on a type it does not know, so this
//! trait names the two operations on the consumer's side; the models implement
//! nothing of the CLI's making.

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use pqbench::v1;

/// A record a level reads on standard input.
pub(crate) trait Record: DeserializeOwned + Serialize {
    /// The stream's noun, for messages.
    const NOUN: &'static str;

    /// Read the record an object or a name spells.
    fn from_json(value: Value) -> Result<Self, String>;

    /// The record's resource name: what a failure is reported under.
    fn name(&self) -> &str;
}

macro_rules! record {
    ($type:ty, $noun:literal) => {
        impl Record for $type {
            const NOUN: &'static str = $noun;

            fn from_json(value: Value) -> Result<Self, String> {
                <$type>::from_json(value)
            }

            fn name(&self) -> &str {
                <$type>::name(self)
            }
        }
    };
}

record!(v1::Catalog, "catalog");
record!(v1::Schema, "schema");
record!(v1::Table, "table");
record!(v1::Partition, "partition");
