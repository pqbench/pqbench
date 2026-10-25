//! A stream's parent record: what a level reads, and the noun a message calls
//! it.

use serde::de::DeserializeOwned;
use serde_json::Value;

/// A level's parent record: a resource read on standard input, and the noun a
/// message calls it.
pub trait Parent: DeserializeOwned + serde::Serialize {
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
        record(
            value,
            Self::NOUN,
            Self::SHAPE,
            Self::from_name,
            Self::name,
            Self::name_mut,
        )
    }

    /// The record as its wire object.
    ///
    /// # Errors
    /// Fails when the record cannot be serialized.
    fn to_json(&self) -> Result<Value, String> {
        serde_json::to_value(self).map_err(|error| error.to_string())
    }
}

/// The body of [`Parent::from_json`]: read the name a value is, or read the
/// object and check the name it carries.
///
/// `named` is the model's own grammar: it builds the record from a resource
/// name or its dotted spelling, and gives `None` for another resource's name.
/// `name_of` reads the `name` an object carries and `name_mut` replaces it, so
/// an object keeps the canonical spelling however its name was written.
fn record<T: DeserializeOwned>(
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
    use super::Parent;
    use crate::v1::{Blob, Metastore, Partition, Table};
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
    fn only_a_resource_with_an_alias_takes_the_dotted_form() {
        assert_eq!(
            Table::parse_name("a.b.c"),
            Some(("a".to_string(), "b".to_string(), "c".to_string()))
        );
        assert_eq!(Blob::parse_name("a.b.c"), None);
        assert_eq!(Metastore::parse_name("m"), None);
        assert_eq!(Metastore::parse_name("metastores/m"), Some("m".to_string()));
    }

    #[test]
    fn a_record_round_trips_through_json() {
        let table = Table::from_json(json!({"name": "a.b.c", "uri": "/lake/c"})).unwrap();
        assert_eq!(
            table.to_json().unwrap()["name"],
            "catalogs/a/schemas/b/tables/c"
        );
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
