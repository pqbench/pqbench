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

pub(super) fn record<T, F, N>(
    value: Value,
    noun: &str,
    shape: &str,
    named: F,
    name_of: N,
) -> Result<T, String>
where
    T: DeserializeOwned,
    F: Fn(&str) -> Option<T>,
    N: Fn(&T) -> &str,
{
    if let Some(name) = value.as_str() {
        return named(name).ok_or_else(|| {
            format!("a {noun} record needs a `{shape}` name or its dotted spelling")
        });
    }
    let record: T = serde_json::from_value(value).map_err(|error| error.to_string())?;
    if named(name_of(&record)).is_none() {
        return Err(format!("a {noun} record needs a `{shape}` name"));
    }
    Ok(record)
}
