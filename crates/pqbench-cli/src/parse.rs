//! Reading a level's parents: the records on standard input.
//!
//! A parent is a record or its name, and the model's `from_json` is the one
//! parser for both. A level names the type it reads; it has no other syntax.

use futures_util::stream::StreamExt;
use pqbench::v1::{self, FromJson};

use crate::source::{self, Records, Reference};
use crate::CliError;

/// What a level reads: the parent's resource type, and how it is named in a
/// failure.
pub(crate) trait Parent: FromJson + Reference + Sized + 'static {
    /// The stream's noun, for messages.
    const NOUN: &'static str;
}

/// The level's parents: the stream on standard input, exactly.
pub(crate) async fn parents<M: Parent>(
    command: &'static str,
) -> Result<Records<'static, M>, CliError> {
    let input = source::read_input(command).await?;
    if !input.piped {
        return Err(format!("{command} reads {} records on standard input", M::NOUN).into());
    }
    Ok(source::records(command, input.first, input.lines)
        .map(|record| record.and_then(decode::<M>))
        .boxed_local())
}

/// One parent record: an object with a `name`, or the name itself.
fn decode<M: Parent>(value: serde_json::Value) -> Result<M, CliError> {
    M::from_json(value).map_err(CliError::from)
}

impl Parent for v1::Catalog {
    const NOUN: &'static str = "catalog";
}

impl Reference for v1::Catalog {
    fn reference(&self) -> &str {
        &self.name
    }
}

impl Parent for v1::Schema {
    const NOUN: &'static str = "schema";
}

impl Reference for v1::Schema {
    fn reference(&self) -> &str {
        &self.name
    }
}

impl Parent for v1::Table {
    const NOUN: &'static str = "table";
}

impl Reference for v1::Table {
    fn reference(&self) -> &str {
        &self.name
    }
}

impl Parent for v1::Partition {
    const NOUN: &'static str = "partition";
}

impl Reference for v1::Partition {
    fn reference(&self) -> &str {
        &self.name
    }
}
