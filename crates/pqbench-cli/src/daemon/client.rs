//! The client side of the lease socket: one request, one response.

use std::fmt;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::protocol::{Request, Response};

/// A failed daemon exchange.
#[derive(Debug)]
pub(crate) enum Error {
    /// The daemon is not there, or the connection broke.
    Connect(std::io::Error),
    /// The daemon answered something that is not this protocol.
    Protocol(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connect(error) => write!(f, "cannot reach the daemon: {error}"),
            Error::Protocol(message) => write!(f, "the daemon answered badly: {message}"),
        }
    }
}

impl std::error::Error for Error {}

/// How long a lease exchange may take: a reader falls back to vending
/// directly when the daemon is slow or gone.
const LEASE_TIMEOUT: Duration = Duration::from_secs(2);

/// How long a catalog exchange may take: the daemon is doing the endpoint
/// and storage read the reader would have done itself.
const CATALOG_TIMEOUT: Duration = Duration::from_secs(120);

pub(crate) async fn request(path: &Path, request: &Request) -> Result<Response, Error> {
    let timeout = match request {
        Request::List { .. } | Request::Get { .. } => CATALOG_TIMEOUT,
        Request::Ping | Request::Stop | Request::Lease { .. } => LEASE_TIMEOUT,
    };
    exchange(path, request, timeout).await
}

async fn exchange(path: &Path, request: &Request, timeout: Duration) -> Result<Response, Error> {
    let mut stream = tokio::time::timeout(timeout, UnixStream::connect(path))
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;
    let line = serde_json::to_vec(request).map_err(|error| Error::Protocol(error.to_string()))?;
    tokio::time::timeout(timeout, stream.write_all(&line))
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;
    tokio::time::timeout(timeout, stream.write_all(b"\n"))
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;
    tokio::time::timeout(timeout, stream.flush())
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    tokio::time::timeout(timeout, reader.read_line(&mut line))
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;
    if line.is_empty() {
        return Err(Error::Protocol(
            "the daemon closed the connection without an answer".into(),
        ));
    }
    serde_json::from_str(&line).map_err(|error| Error::Protocol(error.to_string()))
}

fn timed_out() -> Error {
    Error::Connect(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "the daemon did not answer",
    ))
}

/// Whether a daemon is listening at `path`.
pub(crate) async fn ping(path: &Path) -> bool {
    matches!(
        request(path, &Request::ping()).await,
        Ok(Response { ok: true, .. })
    )
}
