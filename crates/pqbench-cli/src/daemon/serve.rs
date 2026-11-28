//! The daemon: one socket, one cache, one request per connection.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Notify, Semaphore};
use tokio::time::timeout;

use super::path;
use super::protocol::{Request, Response};
use super::session::Daemon;

/// The most connections the daemon answers at once. A connection is one
/// request, so this is also the most requests in flight; a burst beyond it
/// waits in the listener's backlog instead of becoming a task each.
const MAX_CONNECTIONS: usize = 256;

/// How long one connection may take to send its request and read its answer.
/// A stalled client would otherwise hold a task and its file descriptor
/// forever, which is how a daemon runs out of both.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Serve leases and catalog GETs on `socket` until stopped, or until
/// `idle_seconds` pass with no connection (`0` serves until stopped).
pub(crate) async fn run(session: Daemon, socket: PathBuf, idle_seconds: u64) -> Result<(), String> {
    // Listen for the signals before the socket exists: a supervisor may signal
    // as soon as it sees the socket, and a signal that lands before the handler
    // is installed would kill the daemon with its socket file left behind.
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|error| format!("cannot listen for SIGINT: {error}"))?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| format!("cannot listen for SIGTERM: {error}"))?;
    path::prepare(&socket)?;
    let listener = UnixListener::bind(&socket)
        .map_err(|error| format!("cannot bind {}: {error}", socket.display()))?;
    path::set_socket_mode(&socket)?;

    let shutdown = Arc::new(Notify::new());
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let idle = (idle_seconds > 0).then_some(Duration::from_secs(idle_seconds));
    let result = loop {
        let accepted = accept(&listener, idle);
        let stream = tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            _ = interrupt.recv() => break Ok(()),
            _ = terminate.recv() => break Ok(()),
            result = accepted => match result {
                Ok(Some(stream)) => stream,
                Ok(None) => break Ok(()),
                Err(error) => break Err(error),
            },
        };
        // Take a connection slot before spawning, so a burst cannot turn into
        // an unbounded number of tasks each holding a descriptor.
        let permit = tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            _ = interrupt.recv() => break Ok(()),
            _ = terminate.recv() => break Ok(()),
            permit = Arc::clone(&connections).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break Ok(()),
            },
        };
        let session = session.clone();
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let _permit = permit;
            let _ = timeout(CONNECTION_TIMEOUT, handle(stream, session, shutdown)).await;
        });
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
/// the daemon's own bearer — a service-principal one is minted and refreshed
/// here. The status and body go back unchanged, so the reader's own status
/// policy and JSON decoding apply.
async fn answer(session: &Daemon, url: &str) -> Response {
    session.pace().await;
    let workspace = match session.authorized().await {
        Ok(workspace) => workspace,
        Err(error) => {
            return Response {
                error: Some(crate::error::error_chain(error.as_ref())),
                ..Response::default()
            }
        }
    };
    match pqbench::http::fetch(&workspace, url).await {
        Ok(answer) => {
            // A throttled catalog holds the whole daemon's pacer, not just this
            // request, so the next walk does not ask again immediately.
            if answer.status == 429 {
                session.backoff(answer.retry_delay).await;
            }
            Response {
                status: Some(answer.status),
                body: Some(answer.body),
                ..Response::default()
            }
        }
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
    use std::sync::Mutex;

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
            ..pqbench::Config::default()
        })
        .expect("the workspace builds")
    }

    /// What the fake endpoint read from one request: its path, and the
    /// `Authorization` header the daemon sent.
    #[derive(Clone)]
    struct Read {
        path: String,
        authorization: Option<String>,
    }

    /// A fake workspace endpoint: `/oidc/v1/token` mints a bearer living
    /// `expires_in` seconds, every other request answers a `200 {}`. Each
    /// request is recorded, so a test counts the mints and reads the bearer the
    /// daemon sent.
    fn oauth_endpoint(expires_in: u64) -> (String, Arc<Mutex<Vec<Read>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a listener");
        let address = format!("http://{}", listener.local_addr().expect("an address"));
        let reads = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&reads);
        let token = format!(r#"{{"access_token":"minted","expires_in":{expires_in}}}"#);
        std::thread::spawn(move || {
            use std::io::{Read as _, Write};
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut buffer = [0u8; 8192];
                let read = stream.read(&mut buffer).unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                let mut lines = request.lines();
                let path = lines
                    .next()
                    .unwrap_or_default()
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_string();
                let authorization = lines.find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("authorization")
                        .then(|| value.trim().to_string())
                });
                recorded.lock().expect("the record lock").push(Read {
                    path: path.clone(),
                    authorization,
                });
                let body = if path.ends_with("/oidc/v1/token") {
                    token.as_str()
                } else {
                    "{}"
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        (address, reads)
    }

    /// A workspace that authenticates as a service principal.
    fn service_principal(endpoint: &str) -> pqbench::Workspace {
        pqbench::Workspace::new(pqbench::Config {
            endpoint: endpoint.to_string(),
            client_id: Some("client-id".into()),
            client_secret: Some(pqbench::Secret::new("client-secret")),
            ..pqbench::Config::default()
        })
        .expect("the workspace builds")
    }

    /// How many mint requests the endpoint read.
    fn mints(reads: &Arc<Mutex<Vec<Read>>>) -> usize {
        reads
            .lock()
            .expect("the record lock")
            .iter()
            .filter(|read| read.path.ends_with("/oidc/v1/token"))
            .count()
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

    /// A service principal's bearer is minted once and sent to every catalog
    /// request the daemon answers, so a walk's hundreds of GETs cost one token.
    #[tokio::test]
    async fn a_service_principals_token_is_minted_once() {
        let (endpoint, reads) = oauth_endpoint(3600);
        let directory = tempfile::tempdir().expect("a temp directory");
        let socket = directory.path().join("daemon.sock");
        let server = tokio::spawn(run(
            Daemon::direct(service_principal(&endpoint)),
            socket.clone(),
            0,
        ));
        ready(&socket).await;

        let url = format!("{endpoint}/api/2.1/unity-catalog/catalogs");
        for _ in 0..3 {
            let response = client::request(&socket, &Request::http_get(&url))
                .await
                .expect("the daemon answers a get request");
            assert_eq!(response.status, Some(200));
            assert!(response.error.is_none(), "{:?}", response.error);
        }

        assert_eq!(mints(&reads), 1, "the token was minted more than once");
        let reads = reads.lock().expect("the record lock").clone();
        let sent: Vec<&Option<String>> = reads
            .iter()
            .filter(|read| read.path.contains("/catalogs"))
            .map(|read| &read.authorization)
            .collect();
        assert_eq!(sent.len(), 3);
        assert!(
            sent.iter()
                .all(|sent| sent.as_deref() == Some("Bearer minted")),
            "{sent:?}"
        );

        start::stop(&socket).await.expect("the daemon stops");
        server
            .await
            .expect("the server task joins")
            .expect("no server error");
    }

    /// A token with no lifetime left is minted again for the next request: the
    /// daemon never sends an expired token.
    #[tokio::test]
    async fn an_expired_token_is_minted_again() {
        let (endpoint, reads) = oauth_endpoint(0);
        let directory = tempfile::tempdir().expect("a temp directory");
        let socket = directory.path().join("daemon.sock");
        let server = tokio::spawn(run(
            Daemon::direct(service_principal(&endpoint)),
            socket.clone(),
            0,
        ));
        ready(&socket).await;

        let url = format!("{endpoint}/api/2.1/unity-catalog/catalogs");
        for _ in 0..2 {
            let response = client::request(&socket, &Request::http_get(&url))
                .await
                .expect("the daemon answers a get request");
            assert_eq!(response.status, Some(200));
        }
        assert_eq!(mints(&reads), 2, "an expired token was sent again");

        start::stop(&socket).await.expect("the daemon stops");
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
