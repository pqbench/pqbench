//! The daemon: one socket, one cache, one request per connection.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

use super::master::Master;
use super::path;
use super::protocol::{Request, Response};
use super::session::Daemon;

/// The master, when the daemon runs one.
pub(crate) type SharedMaster = Option<Arc<RwLock<Master>>>;

/// Serve leases on `socket` until stopped, or until `idle_seconds` pass with
/// no connection (`0` serves until stopped). A daemon running the master
/// serves until stopped: its work does not come through the socket. On stop
/// the master's map is checkpointed.
pub(crate) async fn run(
    session: Daemon,
    master: SharedMaster,
    socket: PathBuf,
    idle_seconds: u64,
) -> Result<(), String> {
    path::prepare(&socket)?;
    let listener = UnixListener::bind(&socket)
        .map_err(|error| format!("cannot bind {}: {error}", socket.display()))?;
    path::set_socket_mode(&socket)?;

    let shutdown = Arc::new(Notify::new());
    let idle = (idle_seconds > 0 && master.is_none()).then_some(Duration::from_secs(idle_seconds));
    let result = loop {
        let accepted = accept(&listener, idle);
        tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            result = accepted => match result {
                Ok(Some(stream)) => {
                    let session = session.clone();
                    let master = master.clone();
                    let shutdown = Arc::clone(&shutdown);
                    tokio::spawn(async move {
                        let _ = handle(stream, session, master, shutdown).await;
                    });
                }
                Ok(None) => break Ok(()),
                Err(error) => break Err(error),
            }
        }
    };
    // The checkpoint first: a reader that waits for the socket to go away,
    // `daemon stop` included, waits for the map to be on disk.
    if let Some(master) = &master {
        read_master(master).checkpoint()?;
    }
    let _ = std::fs::remove_file(&socket);
    result
}

fn read_master(master: &RwLock<Master>) -> std::sync::RwLockReadGuard<'_, Master> {
    master
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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

async fn handle(
    stream: UnixStream,
    session: Daemon,
    master: SharedMaster,
    shutdown: Arc<Notify>,
) -> Result<(), String> {
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
    if let Request::ListPartitionVersions { parent } = &request {
        let versions = match &master {
            Some(master) => read_master(master).records(parent),
            None => Err("the daemon runs no master; start it with --master".into()),
        };
        return write_versions(write, versions).await;
    }

    let mut response = respond(&request, &session, &shutdown).await;
    if matches!(request, Request::Ping) {
        response.master = master.is_some();
    }

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

/// Answer a `ListPartitionVersions`: the count, then one line per version,
/// written after the master's lock is released.
async fn write_versions(
    write: tokio::net::unix::OwnedWriteHalf,
    versions: Result<Vec<pqbench::v1::PartitionVersion>, String>,
) -> Result<(), String> {
    let header = match &versions {
        Ok(versions) => Response {
            count: Some(versions.len() as u64),
            ..Response::default()
        },
        Err(error) => Response {
            error: Some(error.clone()),
            ..Response::default()
        },
    };
    let mut write = tokio::io::BufWriter::new(write);
    let mut line = serde_json::to_vec(&header).map_err(|error| error.to_string())?;
    for version in versions.iter().flatten() {
        line.push(b'\n');
        write
            .write_all(&line)
            .await
            .map_err(|error| format!("cannot write the versions: {error}"))?;
        line = serde_json::to_vec(version).map_err(|error| error.to_string())?;
    }
    line.push(b'\n');
    write
        .write_all(&line)
        .await
        .map_err(|error| format!("cannot write the versions: {error}"))?;
    write
        .flush()
        .await
        .map_err(|error| format!("cannot flush the versions: {error}"))
}

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
        _ => Response {
            error: Some("not a daemon request".into()),
            ..Response::default()
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
        let server = tokio::spawn(run(cache, None, socket.clone(), 0));

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
