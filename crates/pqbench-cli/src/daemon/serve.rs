//! The daemon: one socket, one cache, one request per connection.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

use super::master::Master;
use super::path;
use super::protocol::{Request, Response};
use super::session::Daemon;

/// The master, when the daemon runs one.
pub(crate) type SharedMaster = Option<Arc<Mutex<Master>>>;

/// How often the master's rows are checkpointed while they change: the most
/// work a crash can cost.
const CHECKPOINT_PERIOD: Duration = Duration::from_secs(10);

/// Serve leases on `socket` until stopped, or until `idle_seconds` pass with
/// no connection (`0` serves until stopped).
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
    let idle = (idle_seconds > 0).then_some(Duration::from_secs(idle_seconds));
    let mut checkpoint = tokio::time::interval(CHECKPOINT_PERIOD);
    let result = loop {
        let accepted = accept(&listener, idle);
        tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            _ = checkpoint.tick() => {
                if let Some(master) = &master {
                    if let Err(error) = lock(master).checkpoint() {
                        break Err(error);
                    }
                }
            }
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
    let _ = std::fs::remove_file(&socket);
    if let Some(master) = &master {
        lock(master).checkpoint()?;
    }
    result
}

fn lock(master: &Mutex<Master>) -> std::sync::MutexGuard<'_, Master> {
    master
        .lock()
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

    let response = match &request {
        Request::Partitions { .. }
        | Request::ScanPath { .. }
        | Request::Scan { .. }
        | Request::RetainTables { .. }
        | Request::Claim { .. }
        | Request::Commit { .. }
        | Request::Revert { .. }
        | Request::Purge => match &master {
            Some(master) => answer(&request, &mut lock(master)),
            None => Response {
                error: Some("the daemon runs no master; start it with --master".into()),
                ..Response::default()
            },
        },
        _ => respond(&request, &session, &shutdown).await,
    };

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

/// Answer one master call; it runs whole while the master is locked.
fn answer(request: &Request, master: &mut Master) -> Response {
    let count = |result: Result<usize, String>| match result {
        Ok(count) => Response {
            count: Some(count as u64),
            ..Response::default()
        },
        Err(error) => Response {
            error: Some(error),
            ..Response::default()
        },
    };
    match request {
        Request::Partitions { table } => Response {
            records: master
                .list(table.as_deref())
                .into_iter()
                .filter_map(|entry| serde_json::to_value(entry).ok())
                .collect(),
            ok: true,
            ..Response::default()
        },
        Request::ScanPath { table } => Response {
            path: Some(master.scan_path(table).to_string_lossy().into_owned()),
            ..Response::default()
        },
        Request::Scan { table } => count(master.scan(table)),
        Request::RetainTables { tables } => count(master.retain_tables(tables)),
        Request::Claim {
            source,
            target,
            limit,
        } => match master.claim(*source, *target, *limit) {
            Ok(claims) => Response {
                records: claims
                    .into_iter()
                    .filter_map(|claim| serde_json::to_value(claim).ok())
                    .collect(),
                ok: true,
                ..Response::default()
            },
            Err(error) => Response {
                error: Some(error),
                ..Response::default()
            },
        },
        Request::Commit {
            name,
            etag,
            source,
            target,
            error,
        } => count(
            master
                .commit(
                    &(name.clone(), etag.clone()),
                    *source,
                    *target,
                    error.clone(),
                )
                .map(usize::from),
        ),
        Request::Revert { state } => count(master.revert(*state)),
        Request::Purge => count(master.purge()),
        _ => Response {
            error: Some("not a master call".into()),
            ..Response::default()
        },
    }
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
