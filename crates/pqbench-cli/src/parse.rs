//! Reading a level's parents: the records on standard input.
//!
//! A parent is a record or its name, and the model's `from_json` is the one
//! parser for both. A level names the type it reads; it has no other syntax.

use futures_util::stream::StreamExt;
use pqbench::v1::Parent;

use crate::source::{self, Records};
use crate::CliError;

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
