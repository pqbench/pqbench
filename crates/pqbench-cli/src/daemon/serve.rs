//! The daemon: one socket, one cache, one request per connection.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

use super::path;
use super::protocol::{Request, Response};
use super::session::Daemon;

/// Serve leases on `socket` until stopped, or until `idle_seconds` pass with
/// no connection (`0` serves until stopped).
pub(crate) async fn run(session: Daemon, socket: PathBuf, idle_seconds: u64) -> Result<(), String> {
    path::prepare(&socket)?;
    let listener = UnixListener::bind(&socket)
        .map_err(|error| format!("cannot bind {}: {error}", socket.display()))?;
    path::set_socket_mode(&socket)?;

    let shutdown = Arc::new(Notify::new());
    let idle = (idle_seconds > 0).then_some(Duration::from_secs(idle_seconds));
    loop {
        let accepted = accept(&listener, idle);
        tokio::select! {
            _ = shutdown.notified() => break,
            result = accepted => match result {
                Ok(Some(stream)) => {
                    let session = session.clone();
                    let shutdown = Arc::clone(&shutdown);
                    tokio::spawn(async move {
                        let _ = handle(stream, session, shutdown).await;
                    });
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = std::fs::remove_file(&socket);
                    return Err(error);
                }
            }
        }
    }
    let _ = std::fs::remove_file(&socket);
    Ok(())
}

async fn accept(
    listener: &UnixListener,
    idle: Option<Duration>,
) -> Result<Option<UnixStream>, String> {
    match idle {
        Some(idle) => match tokio::time::timeout(idle, listener.accept()).await {
            Ok(Ok((stream, _))) => Ok(Some(stream)),
            Ok(Err(error)) => Err(format!("cannot accept a connection: {error}")),
            Err(_) => Ok(None),
        },
        None => listener
            .accept()
            .await
            .map(|(stream, _)| Some(stream))
            .map_err(|error| format!("cannot accept a connection: {error}")),
    }
}

async fn handle(stream: UnixStream, session: Daemon, shutdown: Arc<Notify>) -> Result<(), String> {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    let Some(line) = lines
        .next_line()
        .await
        .map_err(|error| format!("cannot read the request: {error}"))?
    else {
        return Ok(());
    };
    let request: Request =
        serde_json::from_str(&line).map_err(|error| format!("bad request: {error}"))?;

    let response = respond(&request, &session, &shutdown).await;

    let mut line = serde_json::to_string(&response)
        .map_err(|error| format!("cannot encode the answer: {error}"))?;
    line.push('\n');
    write
        .write_all(line.as_bytes())
        .await
        .map_err(|error| format!("cannot write the answer: {error}"))?;
    write
        .flush()
        .await
        .map_err(|error| format!("cannot flush the answer: {error}"))?;
    Ok(())
}

/// Answer one request, recording a failure the reader reports per record.
async fn respond(request: &Request, session: &Daemon, shutdown: &Notify) -> Response {
    match request {
        Request::Stop => {
            shutdown.notify_one();
            Response {
                ok: true,
                ..Response::default()
            }
        }
        Request::Ping => Response {
            ok: true,
            ..Response::default()
        },
        Request::Lease { table } => match session.resolve_vended(table).await {
            Ok(Some(vended)) => Response {
                env: Some(vended.env),
                expire_time_ms: vended.expire_time_ms,
                ..Response::default()
            },
            Ok(None) => Response {
                none: true,
                ..Response::default()
            },
            Err(error) => Response {
                error: Some(crate::error::error_chain(error.as_ref())),
                ..Response::default()
            },
        },
        Request::List { .. } | Request::Get { .. } => match session.records(request).await {
            Ok(records) => Response {
                records,
                ..Response::default()
            },
            Err(error) => Response {
                error: Some(crate::error::error_chain(error.as_ref())),
                ..Response::default()
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::{client, session::Daemon, start};

    #[tokio::test]
    async fn a_ping_and_a_stop_round_trip() {
        let directory = tempfile::tempdir().expect("a temp directory");
        let socket = directory.path().join("daemon.sock");
        let workspace = pqbench::Workspace::new(pqbench::Config {
            endpoint: "http://127.0.0.1:1".into(),
            token: None,
            dialect: pqbench::Dialect::Unity,
            request_timeout: None,
        })
        .expect("the workspace builds");
        let cache = Daemon::direct(workspace);
        let server = tokio::spawn(run(cache, socket.clone(), 0));

        let mut running = false;
        for _ in 0..100 {
            if client::ping(&socket).await {
                running = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(running, "the daemon did not answer a ping");

        let response = client::request(
            &socket,
            &Request::lease("catalogs/local/schemas/local/tables/t"),
        )
        .await
        .expect("the daemon answers a table request");
        assert!(response.none, "a local table should not be vended");

        start::stop(&socket).await.expect("the daemon stops");
        assert!(!socket.exists(), "the socket was not removed");
        server
            .await
            .expect("the server task joins")
            .expect("no server error");
    }
}
