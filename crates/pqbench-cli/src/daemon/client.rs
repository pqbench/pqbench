//! The client side of the daemon socket: one request, one answer.

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

/// How long a ping, a stop, or a lease may take before the reader reports the
/// daemon as gone. A lease waits its turn in the daemon, which paces vends at
/// the catalog's ceiling, so a walk with a stream per CPU queues dozens ahead
/// of the last one; the wait is the daemon's, not a hung connection.
const LEASE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a catalog GET may take: the daemon is sending the request the
/// reader would have sent, and it queues behind the walk's other requests at
/// the catalog's ceiling.
const CATALOG_TIMEOUT: Duration = Duration::from_secs(120);

pub(crate) async fn request(path: &Path, request: &Request) -> Result<Response, Error> {
    let timeout = match request {
        Request::HttpGet { .. } => CATALOG_TIMEOUT,
        Request::Ping | Request::Stop | Request::Lease { .. } => LEASE_TIMEOUT,
    };
    let stream = send(path, request, timeout).await?;
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

/// Write `request` as one line on a fresh connection, ready for the answer.
async fn send(path: &Path, request: &Request, timeout: Duration) -> Result<UnixStream, Error> {
    let mut stream = bounded(timeout, UnixStream::connect(path)).await?;
    let mut line =
        serde_json::to_vec(request).map_err(|error| Error::Protocol(error.to_string()))?;
    line.push(b'\n');
    bounded(timeout, stream.write_all(&line)).await?;
    bounded(timeout, stream.flush()).await?;
    Ok(stream)
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
        request(path, &Request::Ping).await,
        Ok(Response { ok: true, .. })
    )
}
