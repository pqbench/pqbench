//! The client side of the driver socket: one request, one response, and for a
//! version listing the lines after it.

use std::fmt;
use std::future::Future;
use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::UnixStream;

use crate::protocol::{Request, Response};

/// A failed driver exchange.
#[derive(Debug)]
pub enum Error {
    /// The driver is not there, or the connection broke.
    Connect(io::Error),
    /// The driver answered something that is not this protocol.
    Protocol(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connect(error) => write!(f, "cannot reach the driver: {error}"),
            Error::Protocol(message) => write!(f, "the driver answered badly: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// How long a ping or a stop may take before the client reports the driver as
/// gone.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a map or results read may take: the driver writes a checkpoint,
/// or reads a version's result files, while it answers.
const READ_TIMEOUT: Duration = Duration::from_secs(120);

pub async fn request(path: &Path, request: &Request) -> Result<Response, Error> {
    let timeout = match request {
        Request::Ping | Request::Stop | Request::Status => CALL_TIMEOUT,
        Request::ListPartitionVersions { .. }
        | Request::PartitionResults { .. }
        | Request::ListSuggestions { .. } => READ_TIMEOUT,
    };
    Ok(open(path, request, timeout).await?.0)
}

/// Send a version listing `request`: its answer, and the `count` version lines
/// that follow it.
pub async fn list(
    path: &Path,
    request: &Request,
) -> Result<(Response, Lines<BufReader<UnixStream>>), Error> {
    let (response, reader) = open(path, request, READ_TIMEOUT).await?;
    Ok((response, reader.lines()))
}

/// Send `request` and read the answer, leaving the connection open after it.
async fn open(
    path: &Path,
    request: &Request,
    timeout: Duration,
) -> Result<(Response, BufReader<UnixStream>), Error> {
    let mut stream = bounded(timeout, UnixStream::connect(path)).await?;
    let line = serde_json::to_vec(request).map_err(|error| Error::Protocol(error.to_string()))?;
    bounded(timeout, stream.write_all(&line)).await?;
    bounded(timeout, stream.write_all(b"\n")).await?;
    bounded(timeout, stream.flush()).await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    bounded(timeout, reader.read_line(&mut line)).await?;
    if line.is_empty() {
        return Err(Error::Protocol(
            "the driver closed the connection without an answer".into(),
        ));
    }
    let response =
        serde_json::from_str(&line).map_err(|error| Error::Protocol(error.to_string()))?;
    Ok((response, reader))
}

/// One socket operation, bounded by the exchange's timeout.
async fn bounded<T>(
    timeout: Duration,
    operation: impl Future<Output = io::Result<T>>,
) -> Result<T, Error> {
    tokio::time::timeout(timeout, operation)
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)
}

fn timed_out() -> Error {
    Error::Connect(io::Error::new(
        io::ErrorKind::TimedOut,
        "the driver did not answer",
    ))
}

/// Whether a driver is listening at `path`.
pub async fn ping(path: &Path) -> bool {
    matches!(
        request(path, &Request::ping()).await,
        Ok(Response { ok: true, .. })
    )
}
