//! The daemon: one socket, one cache, one request per connection.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

use super::path;
use super::protocol::{Request, Response};
use super::session::Daemon;

/// Serve leases and catalog GETs on `socket` until stopped, or until
/// `idle_seconds` pass with no connection (`0` serves until stopped).
pub(crate) async fn run(session: Daemon, socket: PathBuf, idle_seconds: u64) -> Result<(), String> {
    path::prepare(&socket)?;
    let listener = UnixListener::bind(&socket)
        .map_err(|error| format!("cannot bind {}: {error}", socket.display()))?;
    path::set_socket_mode(&socket)?;

    let shutdown = Arc::new(Notify::new());
    let idle = (idle_seconds > 0).then_some(Duration::from_secs(idle_seconds));
    let result = loop {
        let accepted = accept(&listener, idle);
        tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            result = accepted => match result {
                Ok(Some(stream)) => {
                    let session = session.clone();
                    let shutdown = Arc::clone(&shutdown);
                    tokio::spawn(async move {
                        let _ = handle(stream, session, shutdown).await;
                    });
                }
                Ok(None) => break Ok(()),
                Err(error) => break Err(error),
            }
        }
    };
    let _ = std::fs::remove_file(&socket);
    result
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

/// Answer one connection: one request line, then its answer.
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
    write_frame(&mut write, &respond(&request, &session, &shutdown).await).await
}

async fn write_frame(write: &mut OwnedWriteHalf, frame: &Response) -> Result<(), String> {
    let mut line =
        serde_json::to_vec(frame).map_err(|error| format!("cannot encode the answer: {error}"))?;
    line.push(b'\n');
    write
        .write_all(&line)
        .await
        .map_err(|error| format!("cannot write the answer: {error}"))?;
    write
        .flush()
        .await
        .map_err(|error| format!("cannot flush the answer: {error}"))
}

/// The answer to one request.
async fn respond(request: &Request, session: &Daemon, shutdown: &Notify) -> Response {
    match request {
        Request::Ping => answered(),
        Request::Stop => {
            shutdown.notify_one();
            answered()
        }
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
            Err(error) => failure(error.as_ref()),
        },
        Request::HttpGet { url } => answer(session, url).await,
    }
}

/// One catalog GET: paced at the catalog's ceiling like a vend, and sent under
/// the daemon's own bearer. The status and body go back unchanged, so the
/// reader's own status policy and JSON decoding apply.
async fn answer(session: &Daemon, url: &str) -> Response {
    session.pace().await;
    match pqbench::http::fetch(session.workspace(), url).await {
        Ok(answer) => Response {
            status: Some(answer.status),
            body: Some(answer.body),
            ..Response::default()
        },
        Err(error) => failure(&error),
    }
}

/// The answer that only says the request was served.
fn answered() -> Response {
    Response {
        ok: true,
        ..Response::default()
    }
}

/// The answer that reports `error`, with its cause chain.
fn failure(error: &dyn std::error::Error) -> Response {
    Response {
        error: Some(crate::error::error_chain(error)),
        ..Response::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::{client, start};

    /// A fake endpoint answering every request with `body` as `200`.
    fn endpoint(body: &'static str) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a listener");
        let address = format!("http://{}", listener.local_addr().expect("an address"));
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut buffer = [0u8; 4096];
                let _ = stream.read(&mut buffer);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        address
    }

    fn workspace() -> pqbench::Workspace {
        pqbench::Workspace::new(pqbench::Config {
            endpoint: "http://127.0.0.1:1".into(),
            token: None,
            dialect: pqbench::Dialect::Unity,
            request_timeout: None,
        })
        .expect("the workspace builds")
    }

    async fn ready(socket: &std::path::Path) {
        for _ in 0..100 {
            if client::ping(socket).await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the daemon did not answer a ping");
    }

    #[tokio::test]
    async fn a_ping_and_a_stop_round_trip() {
        let directory = tempfile::tempdir().expect("a temp directory");
        let socket = directory.path().join("daemon.sock");
        let server = tokio::spawn(run(Daemon::direct(workspace()), socket.clone(), 0));
        ready(&socket).await;

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

    /// A catalog GET is sent by the daemon and its answer comes back whole: the
    /// status and the body the endpoint gave, so the reader decodes it.
    #[tokio::test]
    async fn an_http_get_round_trips_the_endpoints_answer() {
        let body = r#"{"catalogs":[{"name":"c1"}]}"#;
        let url = format!("{}/api/2.1/unity-catalog/catalogs", endpoint(body));
        let directory = tempfile::tempdir().expect("a temp directory");
        let socket = directory.path().join("daemon.sock");
        let server = tokio::spawn(run(Daemon::direct(workspace()), socket.clone(), 0));
        ready(&socket).await;

        let response = client::request(&socket, &Request::http_get(&url))
            .await
            .expect("the daemon answers a get request");
        assert_eq!(response.status, Some(200));
        assert_eq!(response.body.as_deref(), Some(body));
        assert!(response.error.is_none());

        start::stop(&socket).await.expect("the daemon stops");
        server
            .await
            .expect("the server task joins")
            .expect("no server error");
    }
}
