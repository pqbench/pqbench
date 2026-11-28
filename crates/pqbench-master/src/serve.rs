//! The master's socket: one request per connection, answered from the map.
//!
//! The master serves its own protocol only — a ping, a stop, the map's
//! versions under a parent, and one version's stored results. The results
//! read runs off the async runtime, so a large version's files never stall a
//! worker command.

use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use pqbench::v1::PartitionVersion;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

use super::master::Master;
use super::protocol::{Request, Response};
use super::socket;
use super::store::Store;
use super::{drive, process, RunArgs};

/// Serve `args`' store on its socket until stopped: the foreground master.
pub(crate) async fn serve(args: &RunArgs) -> Result<(), String> {
    let socket = socket::resolve(args.socket.as_deref(), Some(&args.store()))?;
    let store = Store::open(&args.store())?;
    let master = Master::new(store.read_checkpoint()?, args.low_network)?;
    store.sweep(|key| master.state(key))?;
    let master = Arc::new(RwLock::new(master));
    let exe = process::WorkerExe::open(args.pqbench.as_deref())?;
    drive::spawn(
        master.clone(),
        store.clone(),
        args.settings(),
        exe,
        args.daemon_socket.as_deref().map(socket::absolute),
    );
    run(master, store, socket).await
}

/// Answer requests on `socket` until a stop: on stop the map is checkpointed
/// before the socket goes away, so a reader that waits for that has the map
/// on disk.
pub(crate) async fn run(
    master: Arc<RwLock<Master>>,
    store: Store,
    socket: PathBuf,
) -> Result<(), String> {
    socket::prepare(&socket)?;
    let listener = UnixListener::bind(&socket)
        .map_err(|error| format!("cannot bind {}: {error}", socket.display()))?;
    socket::set_socket_mode(&socket)?;

    let shutdown = Arc::new(Notify::new());
    let result = loop {
        tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let master = master.clone();
                    let store = store.clone();
                    let shutdown = Arc::clone(&shutdown);
                    tokio::spawn(async move {
                        let _ = handle(stream, master, store, shutdown).await;
                    });
                }
                Err(error) => break Err(format!("cannot accept a connection: {error}")),
            }
        }
    };
    store.write_checkpoint(read_master(&master).versions())?;
    let _ = std::fs::remove_file(&socket);
    result
}

/// One version's stored results: `name` is a version, or a partition for its
/// newest live version.
async fn results(
    master: &RwLock<Master>,
    store: &Store,
    name: &str,
) -> Result<Vec<serde_json::Value>, String> {
    let (key, version) = read_master(master).version(name)?;
    let store = store.clone();
    let results = tokio::task::spawn_blocking(move || store.read_results(&key))
        .await
        .map_err(|error| format!("the results read failed: {error}"))??;
    if results.is_empty() {
        let state = serde_json::to_value(version.state).unwrap_or_default();
        return Err(format!(
            "{name} has no results yet ({})",
            state.as_str().unwrap_or_default()
        ));
    }
    Ok(results)
}

fn read_master(master: &RwLock<Master>) -> RwLockReadGuard<'_, Master> {
    master
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn handle(
    stream: UnixStream,
    master: Arc<RwLock<Master>>,
    store: Store,
    shutdown: Arc<Notify>,
) -> Result<(), String> {
    let (read, write) = stream.into_split();
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
    let mut write = BufWriter::new(write);
    match &request {
        Request::Ping => write_frame(&mut write, &ok()).await?,
        Request::Stop => {
            shutdown.notify_one();
            write_frame(&mut write, &ok()).await?;
        }
        Request::ListPartitionVersions { parent } => {
            // The lock is released before a line is written: a slow reader
            // does not hold the map.
            let versions = read_master(&master).records(parent);
            write_versions(&mut write, versions).await?;
        }
        Request::PartitionResults { name } => {
            let response = match results(&master, &store, name).await {
                Ok(records) => Response {
                    records,
                    ..Response::default()
                },
                Err(error) => Response {
                    error: Some(error),
                    ..Response::default()
                },
            };
            write_frame(&mut write, &response).await?;
        }
    }
    write
        .flush()
        .await
        .map_err(|error| format!("cannot flush the answer: {error}"))
}

fn ok() -> Response {
    Response {
        ok: true,
        ..Response::default()
    }
}

/// Answer a `ListPartitionVersions`: the count, then one line per version.
async fn write_versions(
    write: &mut BufWriter<OwnedWriteHalf>,
    versions: Result<Vec<PartitionVersion>, String>,
) -> Result<(), String> {
    match versions {
        Ok(versions) => {
            let count = Some(versions.len() as u64);
            write_frame(
                write,
                &Response {
                    count,
                    ..Response::default()
                },
            )
            .await?;
            for version in &versions {
                write_frame(write, version).await?;
            }
            Ok(())
        }
        Err(error) => {
            let response = Response {
                error: Some(error),
                ..Response::default()
            };
            write_frame(write, &response).await
        }
    }
}

async fn write_frame<T: Serialize>(
    write: &mut BufWriter<OwnedWriteHalf>,
    frame: &T,
) -> Result<(), String> {
    let mut line = serde_json::to_vec(frame).map_err(|error| error.to_string())?;
    line.push(b'\n');
    write
        .write_all(&line)
        .await
        .map_err(|error| format!("cannot write the answer: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client;
    use std::time::Duration;

    #[tokio::test]
    async fn a_ping_and_a_stop_round_trip() {
        let directory = tempfile::tempdir().expect("a temp directory");
        let socket = directory.path().join("master.sock");
        let store = Store::open(directory.path()).expect("the store opens");
        let master = Master::new(Vec::new(), false).expect("an empty map");
        let master = Arc::new(RwLock::new(master));
        let server = tokio::spawn(run(master, store, socket.clone()));

        let mut running = false;
        for _ in 0..100 {
            if client::ping(&socket).await {
                running = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(running, "the master did not answer a ping");

        let response = client::request(&socket, &Request::list_partition_versions(""))
            .await
            .expect("the master answers a listing");
        assert_eq!(response.count, Some(0), "an empty map lists no version");

        let response = client::request(&socket, &Request::partition_results("nothing"))
            .await
            .expect("the master answers a results request");
        assert!(
            response.error.is_some(),
            "a version the map lacks is an error"
        );

        client::request(&socket, &Request::stop())
            .await
            .expect("the master takes a stop");
        for _ in 0..100 {
            if !socket.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!socket.exists(), "the socket was not removed");
        server
            .await
            .expect("the server task joins")
            .expect("no server error");
    }
}
