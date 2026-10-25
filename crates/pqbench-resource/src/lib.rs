//! A pqbench resource on the wire: its name grammar and its JSON form.
//!
//! The derive writes the name methods on the model. This trait is the one
//! contract a generic reader needs, so a consumer (the CLI's stdin levels)
//! can read any resource without the model knowing the reader.

use serde::de::DeserializeOwned;
use serde_json::Value;

pub use pqbench_derive::Resource;

/// A resource on the wire: its name grammar and its JSON form.
pub trait Resource: DeserializeOwned + serde::Serialize {
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

/// The body of [`Resource::from_json`]: read the name a value is, or read the
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
