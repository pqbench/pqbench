//! Reading a record: the wire object, or just its name.
//!
//! A parent arrives as an object with a `name`, or as a JSON string that is
//! either the resource name or its dotted spelling (`cat.schema.table`).
//! [`FromJson`] is the one parser for both; the spelling itself lives in
//! [`super::name`].

use serde::de::DeserializeOwned;
use serde_json::Value;

/// A record read from a JSON value.
pub trait FromJson: Sized {
    /// Read the record an object or a name spells.
    ///
    /// # Errors
    /// Fails when the value is not the record's shape, or its `name` is not
    /// this resource's name or dotted spelling.
    fn from_json(value: Value) -> Result<Self, String>;
}

/// The body every [`FromJson`] shares: read the name a value is, or read the
/// object and check the name it carries.
///
/// `named` is the model's own grammar: it builds the record from a resource
/// name or its dotted spelling, and gives `None` for another resource's name.
/// `name_of` reads the `name` an object carries and `name_mut` replaces it, so
/// an object keeps the canonical spelling however its name was written.
pub(super) fn record<T: DeserializeOwned>(
    value: Value,
    noun: &str,
    shape: &str,
    named: impl Fn(&str) -> Option<T>,
    name_of: impl Fn(&T) -> &str,
    name_mut: impl Fn(&mut T) -> &mut String,
) -> Result<T, String> {
    if let Some(name) = value.as_str() {
        return named(name).ok_or_else(|| {
            format!("a {noun} record needs a `{shape}` name or its dotted spelling")
        });
    }
    let mut record: T = serde_json::from_value(value).map_err(|error| error.to_string())?;
    let Some(built) = named(name_of(&record)) else {
        return Err(format!("a {noun} record needs a `{shape}` name"));
    };
    *name_mut(&mut record) = name_of(&built).to_owned();
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::FromJson;
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
