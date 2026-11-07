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
            Error::Connect(error) => write!(f, "cannot reach the lease daemon: {error}"),
            Error::Protocol(message) => write!(f, "the lease daemon answered badly: {message}"),
        }
    }
}

impl std::error::Error for Error {}

const TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) async fn request(path: &Path, request: &Request) -> Result<Response, Error> {
    let mut stream = tokio::time::timeout(TIMEOUT, UnixStream::connect(path))
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;
    let line = serde_json::to_vec(request).map_err(|error| Error::Protocol(error.to_string()))?;
    tokio::time::timeout(TIMEOUT, stream.write_all(&line))
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;
    tokio::time::timeout(TIMEOUT, stream.write_all(b"\n"))
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;
    tokio::time::timeout(TIMEOUT, stream.flush())
        .await
        .map_err(|_| timed_out())?
        .map_err(Error::Connect)?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    tokio::time::timeout(TIMEOUT, reader.read_line(&mut line))
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
        "the lease daemon did not answer",
    ))
}

/// Whether a daemon is listening at `path`.
pub(crate) async fn ping(path: &Path) -> bool {
    matches!(
        request(path, &Request::ping()).await,
        Ok(Response { ok: true, .. })
    )
}
