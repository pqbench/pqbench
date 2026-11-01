//! Documents on stdin or a file. The resource's name decides what it is.
//!
//! A [`v1::TableFile`] names the blob to read and carries its lease; the
//! bytemass kinds are its own output. The `name`'s collection segment
//! identifies each record (AIP-122), so no record carries a `kind`. A pipe
//! writes NDJSON; a terminal prints an aligned table (override with
//! `--format`).

use futures_util::stream::{self, BoxStream, Stream, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader, Lines, Stdin};

use pqbench::v1;

use crate::stdin::is_stream;
use crate::CliError;

/// One JSON value from a table-file stream or a one-object document.
pub(crate) enum Record {
    /// One table file: its blob and the lease to read it.
    File(v1::TableFile),
    /// One measured file's facts.
    Blob(v1::Blob),
    /// One file column and its byte mass.
    Column(v1::Column),
}

/// The records of a document, one at a time: `-` streams standard input, a path
/// is read whole (documents are metadata, not data). Each value is classified
/// as it arrives, so a command is a filter over the stream and never buffers
/// its input.
pub(crate) async fn records(
    input: &str,
) -> Result<BoxStream<'static, Result<Record, CliError>>, CliError> {
    if input == "-" {
        if !is_stream() {
            return Err("standard input is not a stream; pass a file or pipe a document in".into());
        }
        Ok(stdin_records().boxed())
    } else {
        let bytes = tokio::fs::read(input).await?;
        let bytes = if bytes.starts_with(&LZ4_MAGIC) {
            decode_lz4(&bytes)?
        } else {
            bytes
        };
        Ok(stream::iter(file_records(bytes)).boxed())
    }
}

/// Read standard input one value at a time. A value may span lines, so lines
/// accumulate until one parses.
fn stdin_records() -> impl Stream<Item = Result<Record, CliError>> {
    struct State {
        lines: Lines<BufReader<Stdin>>,
        buffer: String,
        empty: bool,
        done: bool,
    }
    let state = State {
        lines: BufReader::new(tokio::io::stdin()).lines(),
        buffer: String::new(),
        empty: true,
        done: false,
    };
    stream::try_unfold(state, |mut state| async move {
        if state.done {
            return Ok(None);
        }
        loop {
            match state.lines.next_line().await? {
                Some(line) => {
                    state.buffer.push_str(&line);
                    state.buffer.push('\n');
                    match serde_json::from_str::<serde_json::Value>(&state.buffer) {
                        Ok(value) => {
                            state.empty = false;
                            state.buffer.clear();
                            return Ok(Some((classify(value)?, state)));
                        }
                        Err(error) if error.is_eof() => {}
                        Err(error) => return Err(invalid_json(error)),
                    }
                }
                None => {
                    state.done = true;
                    if !state.buffer.trim().is_empty() {
                        return Err("incomplete document".into());
                    }
                    if state.empty {
                        return Err("empty document".into());
                    }
                    return Ok(None);
                }
            }
        }
    })
}

/// The values of a whole document file, classified in order.
fn file_records(bytes: Vec<u8>) -> Vec<Result<Record, CliError>> {
    let mut records = Vec::new();
    for value in serde_json::Deserializer::from_reader(std::io::Cursor::new(bytes))
        .into_iter::<serde_json::Value>()
    {
        match value {
            Ok(value) => records.push(classify(value)),
            Err(error) => {
                records.push(Err(invalid_json(error)));
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
fn classify(value: serde_json::Value) -> Result<Record, CliError> {
    let name = value
        .get("name")
        .and_then(|name| name.as_str())
        .map(str::to_owned);
    match name.as_deref() {
        Some(name) if v1::Column::parse_name(name).is_some() => {
            Ok(Record::Column(from_value(value)?))
        }
        Some(name) if v1::name::parse_blob(name).is_some() => Ok(Record::Blob(from_value(value)?)),
        Some(name) if v1::name::parse_table_file(name).is_some() => {
            Ok(Record::File(parse_file(value)?))
        }
        Some(name) => Err(invalid_name(name)),
        None => Err("a document record needs a name".into()),
    }
}

fn from_value<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, CliError> {
    serde_json::from_value(value).map_err(invalid_json)
}

/// A table-file resource: the blob it names is what gets read.
fn parse_file(value: serde_json::Value) -> Result<v1::TableFile, CliError> {
    let file: v1::TableFile = from_value(value)?;
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

/// Whether a path is `-`, JSON (`{`), or an lz4 frame.
pub(crate) async fn is_document(path: &str) -> bool {
    if path == "-" {
        return true;
    }
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

pub(crate) fn invalid_json(error: serde_json::Error) -> CliError {
    format!(
        "invalid pqbench document at line {}, column {}",
        error.line(),
        error.column()
    )
    .into()
}

fn invalid_name(name: &str) -> CliError {
    format!("unsupported document name `{name}`; expected a table-file, a blob, or a bytemass")
        .into()
}
