//! Documents on stdin or a file. The resource's name decides what it is.
//!
//! A [`v1::TableFile`] names the blob to read; its lease is session state and
//! never on the record. The bytemass kinds are its own output. The `name`'s collection segment
//! identifies each record (AIP-122), so no record carries a `kind`. A pipe
//! writes NDJSON; a terminal prints an aligned table (override with
//! `--format`). A summarized column row names no resource, so it is the one
//! record recognized by its shape instead.
//!
//! A value may span lines, so the reader tries the bytes it has before reading
//! more and yields a value as soon as one is complete: `serde_json`'s stream
//! deserializer is the only parser that says where a value ends and which
//! failures mean "not yet", so it is what splits a document. The bytes come
//! back to a buffer that is reused across records. A record is then read from
//! the parsed value (`T::deserialize(&value)`), not from a moved copy of it.

use std::path::Path;

use futures_util::stream::{self, BoxStream, Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader, Stdin};

use pqbench::mass;
use pqbench::v1;

use crate::stdin::is_stream;
use crate::CliError;

/// One JSON value from a table-file stream or a one-object document.
pub(crate) enum Record {
    /// One table file: the blob to read, without a lease.
    File(v1::TableFile),
    /// One measured file's facts; structural, not a row.
    Blob,
    /// One file column and its byte mass. Boxed: a column record is much
    /// larger than a table-file one, and a stream holds one at a time.
    Column(Box<v1::Column>),
    /// One summarized table column: a document with no addressable name.
    Mass(mass::ColumnMass),
}

/// Where a command's document comes from.
#[derive(Clone, Copy)]
pub(crate) enum Input<'a> {
    /// Standard input, read one value at a time.
    Stdin,
    /// A path, read whole: documents are metadata, not data.
    File(&'a Path),
}

/// The records of a document, one at a time. Each value is classified as it
/// arrives, so a command is a filter over the stream and never buffers its
/// input.
pub(crate) async fn records(
    input: Input<'_>,
) -> Result<BoxStream<'static, Result<Record, CliError>>, CliError> {
    match input {
        Input::Stdin => {
            if !is_stream() {
                return Err(
                    "standard input is not a stream; pass a file or pipe a document in".into(),
                );
            }
            Ok(stdin_records().boxed())
        }
        Input::File(path) => {
            let bytes = tokio::fs::read(path).await?;
            let bytes = if bytes.starts_with(&LZ4_MAGIC) {
                decode_lz4(&bytes)?
            } else {
                bytes
            };
            Ok(stream::iter(file_records(bytes)).boxed())
        }
    }
}

/// Read standard input one value at a time.
fn stdin_records() -> impl Stream<Item = Result<Record, CliError>> {
    struct State {
        input: BufReader<Stdin>,
        /// Bytes read but not consumed yet: one value, or a prefix of one.
        bytes: Vec<u8>,
        /// Whether a record has been yielded.
        any: bool,
    }
    let state = State {
        input: BufReader::new(tokio::io::stdin()),
        bytes: Vec::new(),
        any: false,
    };
    stream::try_unfold(state, |mut state| async move {
        loop {
            // The buffer is tried before more is read: a value that arrived
            // whole is yielded without waiting for a line it does not need.
            if let Some((value, used)) = next_value(&state.bytes)? {
                state.bytes.drain(..used);
                state.any = true;
                let record = classify(&value)?;
                return Ok(Some((record, state)));
            }
            if state.input.read_until(b'\n', &mut state.bytes).await? == 0 {
                if !state.bytes.iter().all(u8::is_ascii_whitespace) {
                    return Err("incomplete document".into());
                }
                if !state.any {
                    return Err("empty document".into());
                }
                return Ok(None);
            }
        }
    })
}

/// The first value in `buffer` and the bytes it takes, when the buffer holds
/// one; a prefix holds none yet.
fn next_value(buffer: &[u8]) -> Result<Option<(Value, usize)>, CliError> {
    let mut values = serde_json::Deserializer::from_slice(buffer).into_iter::<Value>();
    match values.next() {
        Some(Ok(value)) => Ok(Some((value, values.byte_offset()))),
        // Not a failure: the document has not arrived yet.
        Some(Err(error)) if error.is_eof() => Ok(None),
        Some(Err(error)) => Err(invalid_json(error.line(), error.column())),
        None => Ok(None),
    }
}

/// The values of a whole document file, classified in order.
fn file_records(bytes: Vec<u8>) -> Vec<Result<Record, CliError>> {
    let mut records = Vec::new();
    for value in serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>() {
        match value {
            Ok(value) => records.push(classify(&value)),
            Err(error) => {
                records.push(Err(invalid_json(error.line(), error.column())));
                return records;
            }
        }
    }
    if records.is_empty() {
        records.push(Err("empty document".into()));
    }
    records
}

/// Classify one value by the collection segment of its `name` (AIP-122).
fn classify(value: &Value) -> Result<Record, CliError> {
    let name = value.get("name").and_then(Value::as_str);
    match name {
        Some(name) if v1::Column::parse_name(name).is_some() => {
            Ok(Record::Column(Box::new(decode(value)?)))
        }
        Some(name) if v1::name::parse_blob(name).is_some() => {
            let _: v1::Blob = decode(value)?;
            Ok(Record::Blob)
        }
        Some(name) if v1::name::parse_table_file(name).is_some() => {
            Ok(Record::File(parse_file(value)?))
        }
        Some(name) => Err(invalid_name(name)),
        None => match mass::ColumnMass::from_document(value) {
            Some(row) => Ok(Record::Mass(row)),
            None => Err("a document record needs a name or a summarized column".into()),
        },
    }
}

/// The record of the curated type that holds `value`.
fn decode<T: DeserializeOwned>(value: &Value) -> Result<T, CliError> {
    T::deserialize(value).map_err(|error| invalid_json(error.line(), error.column()))
}

/// A table-file resource: the blob it names is what gets read.
fn parse_file(value: &Value) -> Result<v1::TableFile, CliError> {
    let file: v1::TableFile = decode(value)?;
    if file.blob.is_none() {
        return Err("a table-file record needs a blob to read".into());
    }
    Ok(file)
}

const LZ4_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

/// Decode an lz4 frame whole; documents are metadata, not data.
fn decode_lz4(bytes: &[u8]) -> Result<Vec<u8>, CliError> {
    let mut decoder = lz4::Decoder::new(std::io::Cursor::new(bytes))?;
    let mut decoded = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut decoded)?;
    Ok(decoded)
}

/// Whether a path is JSON (`{`) or an lz4 frame.
pub(crate) async fn is_document(path: &Path) -> bool {
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return false;
    };
    let mut buf = [0u8; 64];
    let Ok(n) = file.read(&mut buf).await else {
        return false;
    };
    if n >= 4 && buf[..4] == LZ4_MAGIC {
        return true;
    }
    buf[..n]
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        == Some(b'{')
}

pub(crate) fn invalid_json(line: usize, column: usize) -> CliError {
    format!("invalid pqbench document at line {line}, column {column}").into()
}

fn invalid_name(name: &str) -> CliError {
    format!(
        "unsupported document name `{name}`; expected a table-file, a blob, a column, or a summarized column"
    )
    .into()
}
