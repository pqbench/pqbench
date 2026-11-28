//! The driver's socket: one request per connection, answered from the map.
//!
//! The driver serves its own protocol only — a ping, a stop, the map's
//! versions under a parent, one version's stored results, the map's state, and
//! the suggestions it measured. A read runs off the async runtime, so a large
//! version's files never stall a worker command.
//!
//! The foreground driver ends on a stop request or on `SIGINT`/`SIGTERM`: it
//! stops the runs in flight, checkpoints the map, removes its socket, and exits
//! 0.

use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use pqbench::v1::{PartitionVersion, Suggestion};
use serde::Serialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Notify;

use super::drive::Task;
use super::engine::Engine;
use super::protocol::{Request, Response};
use super::run_control;
use super::socket;
use super::store::Store;
use super::{drive, process, RunArgs};

/// Serve `args`' store on its socket until stopped: the foreground driver.
pub(crate) async fn serve(args: &RunArgs) -> Result<(), String> {
    let socket = socket::resolve(args.socket.as_deref(), Some(&args.store()))?;
    let run_control = run_control::read()?;
    let store = Store::open(&args.store(), run_control.checkpoint_codec)?;
    let engine = Engine::new(store.read_checkpoint()?, args.low_network)?;
    store.sweep(|key| engine.state(key))?;
    let engine = Arc::new(RwLock::new(engine));
    let worker = process::Worker::new(
        process::WorkerExe::open(args.pqbench.as_deref())?,
        args.daemon_socket.as_deref().map(socket::absolute),
        run_control.step_timeout,
    );
    let driver = drive::spawn(engine.clone(), store.clone(), args.settings(), worker);
    run(engine, store, socket, driver).await
}

/// Answer requests on `socket` until a stop or a signal. The driver is stopped
/// first, so its runs never outlive the map they move; then the map is
/// checkpointed and the socket goes away, so a reader that waits for that has
/// the map on disk.
pub(crate) async fn run(
    engine: Arc<RwLock<Engine>>,
    store: Store,
    socket: PathBuf,
    driver: Task,
) -> Result<(), String> {
    // Listen for the signals before the socket exists: a supervisor may signal
    // as soon as it sees the socket, and a signal that lands before the handler
    // is installed would kill the process with no checkpoint and no cleanup.
    let mut interrupt = signal(SignalKind::interrupt())
        .map_err(|error| format!("cannot listen for SIGINT: {error}"))?;
    let mut terminate = signal(SignalKind::terminate())
        .map_err(|error| format!("cannot listen for SIGTERM: {error}"))?;
    socket::prepare(&socket, "driver")?;
    let listener = UnixListener::bind(&socket)
        .map_err(|error| format!("cannot bind {}: {error}", socket.display()))?;
    socket::set_socket_mode(&socket)?;

    let shutdown = Arc::new(Notify::new());
    let result = loop {
        tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            _ = interrupt.recv() => break Ok(()),
            _ = terminate.recv() => break Ok(()),
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let engine = engine.clone();
                    let store = store.clone();
                    let shutdown = Arc::clone(&shutdown);
                    tokio::spawn(async move {
                        let _ = handle(stream, engine, store, shutdown).await;
                    });
                }
                Err(error) => break Err(format!("cannot accept a connection: {error}")),
            }
        }
    };
    driver.stop().await;
    let checkpoint = store.write_checkpoint(read_engine(&engine).versions());
    let _ = std::fs::remove_file(&socket);
    checkpoint.and(result)
}

/// One version's stored results: `name` is a version, or a partition for its
/// newest live version.
async fn results(
    engine: &RwLock<Engine>,
    store: &Store,
    name: &str,
) -> Result<Vec<serde_json::Value>, String> {
    let (key, version) = read_engine(engine).version(name)?;
    let store = store.clone();
    let results = tokio::task::spawn_blocking(move || store.read_results(&key))
        .await
        .map_err(|error| format!("the results read failed: {error}"))??;
    if results.is_empty() {
        return Err(format!("{name} has no results yet ({})", version.state));
    }
    Ok(results)
}

/// The suggestions the live versions under `parent` hold, largest saving
/// first.
async fn suggestions(
    engine: &RwLock<Engine>,
    store: &Store,
    parent: Option<&str>,
) -> Result<Vec<Value>, String> {
    let keys = read_engine(engine).keys(parent.unwrap_or_default())?;
    let store = store.clone();
    let mut suggestions = tokio::task::spawn_blocking(move || {
        let mut all = Vec::new();
        for key in keys {
            all.extend(store.read_suggestions(&key)?);
        }
        Ok::<Vec<Suggestion>, String>(all)
    })
    .await
    .map_err(|error| format!("the suggestions read failed: {error}"))??;
    suggestions.sort_by(|left, right| {
        right
            .savings_bytes
            .cmp(&left.savings_bytes)
            .then_with(|| left.name.cmp(&right.name))
    });
    suggestions
        .iter()
        .map(|suggestion| serde_json::to_value(suggestion).map_err(|error| error.to_string()))
        .collect()
}

fn read_engine(engine: &RwLock<Engine>) -> RwLockReadGuard<'_, Engine> {
    engine
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

async fn handle(
    stream: UnixStream,
    engine: Arc<RwLock<Engine>>,
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
            let versions = read_engine(&engine).records(parent);
            write_versions(&mut write, versions).await?;
        }
        Request::PartitionResults { name } => {
            let response = match results(&engine, &store, name).await {
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
        Request::Status => {
            // The lock is released before a line is written: a slow reader
            // does not hold the map.
            let status = read_engine(&engine).status();
            let response = Response {
                status: Some(status),
                ..Response::default()
            };
            write_frame(&mut write, &response).await?;
        }
        Request::ListSuggestions { parent } => {
            let response = match suggestions(&engine, &store, parent.as_deref()).await {
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
    use crate::drive::Settings;
    use crate::process::{Worker, WorkerExe};
    use crate::store::CheckpointCodec;
    use std::path::Path;
    use std::time::Duration;

    /// A driver task over `engine` and `store` that runs no command: the
    /// tests here cover the socket, not the transitions.
    fn idle_driver(engine: Arc<RwLock<Engine>>, store: Store) -> Task {
        let worker = Worker::new(
            WorkerExe::open(Some(Path::new("/bin/true"))).expect("/bin/true opens"),
            None,
            Duration::from_secs(30),
        );
        drive::spawn(
            engine,
            store,
            Settings {
                walk_tables: None,
                walk_period: None,
            },
            worker,
        )
    }

    #[tokio::test]
    async fn a_ping_and_a_stop_round_trip() {
        let directory = tempfile::tempdir().expect("a temp directory");
        let socket = directory.path().join("driver.sock");
        let store = Store::open(directory.path(), CheckpointCodec::None).expect("the store opens");
        let engine = Engine::new(Vec::new(), false).expect("an empty map");
        let engine = Arc::new(RwLock::new(engine));
        let driver = idle_driver(engine.clone(), store.clone());
        let server = tokio::spawn(run(engine, store, socket.clone(), driver));

        let mut running = false;
        for _ in 0..100 {
            if client::ping(&socket).await {
                running = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(running, "the driver did not answer a ping");

        let response = client::request(&socket, &Request::list_partition_versions(""))
            .await
            .expect("the driver answers a listing");
        assert_eq!(response.count, Some(0), "an empty map lists no version");

        let response = client::request(&socket, &Request::partition_results("nothing"))
            .await
            .expect("the driver answers a results request");
        assert!(
            response.error.is_some(),
            "a version the map lacks is an error"
        );

        let response = client::request(&socket, &Request::status())
            .await
            .expect("the driver answers a status request");
        let status = response.status.expect("a status");
        assert_eq!(status.total_versions, 0);
        assert_eq!(status.failing_versions, 0);
        assert_eq!(status.state_counts["UP_TO_DATE"], 0);

        let response = client::request(&socket, &Request::list_suggestions(None))
            .await
            .expect("the driver answers a suggestion request");
        assert!(response.records.is_empty(), "an empty map suggests nothing");

        client::request(&socket, &Request::stop())
            .await
            .expect("the driver takes a stop");
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
