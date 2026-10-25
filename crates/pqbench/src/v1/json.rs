//! Reading a record: the wire object, or just its name.
//!
//! A parent arrives as an object with a `name`, or as a JSON string that is
//! either the resource name or its dotted spelling (`cat.schema.table`).
//! [`Parent::from_json`](super::Parent) is the one parser for both; the
//! spelling itself lives in [`super::name`].

use serde::de::DeserializeOwned;
use serde_json::Value;

/// The body of [`Parent::from_json`](super::Parent): read the name a value
/// is, or read the object and check the name it carries.
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
