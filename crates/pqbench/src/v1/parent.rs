//! A stream's parent record: what a level reads, and the noun a message calls
//! it.

use serde::de::DeserializeOwned;
use serde_json::Value;

use super::json;

/// A level's parent record: a resource read on standard input, and the noun a
/// message calls it.
pub trait Parent: DeserializeOwned {
    /// The stream's noun, for messages.
    const NOUN: &'static str;

    /// The name's shape, for messages.
    const SHAPE: &'static str;

    /// The record a name spells: the resource name or its dotted shorthand.
    fn from_name(name: &str) -> Option<Self>;

    /// The record's resource name: what a failure is reported under.
    fn name(&self) -> &str;

    /// The record's resource name, to replace a dotted spelling with the
    /// canonical one.
    fn name_mut(&mut self) -> &mut String;

    /// Read the record an object or a name spells.
    ///
    /// # Errors
    /// Fails when the value is not the record's shape, or its `name` is not
    /// this resource's name or dotted spelling.
    fn from_json(value: Value) -> Result<Self, String> {
        json::record(
            value,
            Self::NOUN,
            Self::SHAPE,
            Self::from_name,
            Self::name,
            Self::name_mut,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Parent;
    use crate::v1::{Partition, Table};
    use serde_json::json;

    #[test]
    fn a_dotted_object_name_is_normalized() {
        let table = Table::from_json(json!({"name": "a.b.c", "uri": "/lake/c"})).unwrap();
        assert_eq!(table.name, "catalogs/a/schemas/b/tables/c");
        assert_eq!(table.uri.as_deref(), Some("/lake/c"));
    }

    #[test]
    fn a_canonical_object_name_is_kept() {
        let table = Table::from_json(json!({"name": "catalogs/a/schemas/b/tables/c"})).unwrap();
        assert_eq!(table.name, "catalogs/a/schemas/b/tables/c");
    }

    #[test]
    fn a_dotted_string_names_a_partition() {
        let partition = Partition::from_json(json!("a.b.c.2026-01-01")).unwrap();
        assert_eq!(
            partition.name,
            "catalogs/a/schemas/b/tables/c/partitions/2026-01-01"
        );
    }
}
