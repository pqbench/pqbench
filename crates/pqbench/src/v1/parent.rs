//! A stream's parent record: what a level reads, and the noun a message calls
//! it.

use super::json::FromJson;

/// A level's parent record: a resource read on standard input, and the noun a
/// message calls it.
pub trait Parent: FromJson {
    /// The stream's noun, for messages.
    const NOUN: &'static str;

    /// The record's resource name: what a failure is reported under.
    fn name(&self) -> &str;
}
