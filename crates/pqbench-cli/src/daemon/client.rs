//! The client side of the lease socket: one request, one response.

use std::fmt;
use std::future::Future;
use std::io;
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::protocol::{Request, Response};

/// A failed daemon exchange.
#[derive(Debug)]
pub(crate) enum Error {
    /// The daemon is not there, or the connection broke.
    Connect(io::Error),
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
            "the daemon closed the connection without an answer".into(),
        ));
    }
    serde_json::from_str(&line).map_err(|error| Error::Protocol(error.to_string()))
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
