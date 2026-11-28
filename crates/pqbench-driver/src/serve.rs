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
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use pqbench_model_v1::PartitionVersion;
use serde::Serialize;
use serde_json::value::{to_raw_value, RawValue};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{Notify, Semaphore};
use tokio::time::timeout;

use super::drive::Task;
use super::engine::Engine;
use super::protocol::{Request, Response};
use super::run_control;
use super::socket;
use super::store::Store;
use super::{drive, process, RunArgs};

/// The most connections the driver answers at once. A connection is one
/// request, so this is also the most requests in flight; a burst beyond it
/// waits in the listener's backlog instead of becoming a task each.
const MAX_CONNECTIONS: usize = 64;

/// How long one connection may take to send its request and read its answer.
/// A stalled client would otherwise hold a task and its file descriptor
/// forever.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Serve `args`' store on its socket until stopped: the foreground driver.
pub(crate) async fn serve(args: &RunArgs) -> Result<(), String> {
    let socket = socket::resolve(args.socket.as_deref(), Some(&args.store()))?;
    let run_control = run_control::read()?;
    let store = Store::open(&args.store())?;
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
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let result = loop {
        let stream = tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            _ = interrupt.recv() => break Ok(()),
            _ = terminate.recv() => break Ok(()),
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => break Err(format!("cannot accept a connection: {error}")),
            }
        };
        // Take a slot before spawning, so a burst cannot turn into an
        // unbounded number of tasks each holding a descriptor.
        let permit = tokio::select! {
            _ = shutdown.notified() => break Ok(()),
            _ = interrupt.recv() => break Ok(()),
            _ = terminate.recv() => break Ok(()),
            permit = Arc::clone(&connections).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break Ok(()),
            },
        };
        let engine = engine.clone();
        let store = store.clone();
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let _permit = permit;
            let _ = timeout(CONNECTION_TIMEOUT, handle(stream, engine, store, shutdown)).await;
        });
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
) -> Result<Vec<Box<RawValue>>, String> {
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

/// The suggestions the `UP_TO_DATE` versions under `parent` hold, largest
/// saving first. A version's suggestions are read from its results once and
/// then held by the map, so a lake-wide listing costs the reads the map has not
/// done yet; a version still in the pipeline holds no results to read.
async fn suggestions(
    engine: &RwLock<Engine>,
    store: &Store,
    parent: Option<&str>,
) -> Result<Vec<Box<RawValue>>, String> {
    let keys = read_engine(engine).measured_keys(parent.unwrap_or_default())?;
    let mut suggestions = Vec::new();
    let mut missing = Vec::new();
    {
        let engine = read_engine(engine);
        for key in keys {
            match engine.cached_suggestions(&key) {
                Some(cached) => suggestions.extend_from_slice(cached),
                None => missing.push(key),
            }
        }
    }
    if !missing.is_empty() {
        let store = store.clone();
        let read = tokio::task::spawn_blocking(move || {
            missing
                .into_iter()
                .map(|key| {
                    let suggestions = store.read_suggestions(&key)?;
                    Ok((key, suggestions))
                })
                .collect::<Result<Vec<_>, String>>()
        })
        .await
        .map_err(|error| format!("the suggestions read failed: {error}"))??;
        let mut engine = write_engine(engine);
        for (key, held) in read {
            suggestions.extend(held.iter().cloned());
            engine.cache_suggestions(&key, held);
        }
    }
    suggestions.sort_by(|left, right| {
        right
            .savings_bytes
            .cmp(&left.savings_bytes)
            .then_with(|| left.name.cmp(&right.name))
    });
    suggestions
        .iter()
        .map(|suggestion| to_raw_value(suggestion).map_err(|error| error.to_string()))
        .collect()
}

fn read_engine(engine: &RwLock<Engine>) -> RwLockReadGuard<'_, Engine> {
    engine
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_engine(engine: &RwLock<Engine>) -> RwLockWriteGuard<'_, Engine> {
    engine
        .write()
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
    // The protocol is `serde_json`'s: a pass-through record is a `RawValue`,
    // which only a direct deserializer can read.
    let request: Request =
        serde_json::from_str(&line).map_err(|error| format!("bad request: {error}"))?;
    let mut write = BufWriter::new(write);
    let mut frame = Vec::new();
    match &request {
        Request::Ping => write_frame(&mut write, &mut frame, &ok()).await?,
        Request::Stop => {
            shutdown.notify_one();
            write_frame(&mut write, &mut frame, &ok()).await?;
        }
        Request::ListPartitionVersions { parent } => {
            // The lock is released before a line is written: a slow reader
            // does not hold the map.
            let versions = read_engine(&engine).records(parent);
            write_versions(&mut write, &mut frame, versions).await?;
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
            write_frame(&mut write, &mut frame, &response).await?;
        }
        Request::Status => {
            // The lock is released before a line is written: a slow reader
            // does not hold the map.
            let status = read_engine(&engine).status();
            let response = Response {
                status: Some(status),
                ..Response::default()
            };
            write_frame(&mut write, &mut frame, &response).await?;
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
            write_frame(&mut write, &mut frame, &response).await?;
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
    frame: &mut Vec<u8>,
    versions: Result<Vec<PartitionVersion>, String>,
) -> Result<(), String> {
    match versions {
        Ok(versions) => {
            let count = Some(versions.len() as u64);
            write_frame(
                write,
                frame,
                &Response {
                    count,
                    ..Response::default()
                },
            )
            .await?;
            for version in &versions {
                write_frame(write, frame, version).await?;
            }
            Ok(())
        }
        Err(error) => {
            let response = Response {
                error: Some(error),
                ..Response::default()
            };
            write_frame(write, frame, &response).await
        }
    }
}

/// One answer, written from a reused buffer: a listing can be millions of
/// lines, and none of them needs a buffer of its own.
async fn write_frame<T: Serialize>(
    write: &mut BufWriter<OwnedWriteHalf>,
    frame: &mut Vec<u8>,
    answer: &T,
) -> Result<(), String> {
    frame.clear();
    serde_json::to_writer(&mut *frame, answer).map_err(|error| error.to_string())?;
    frame.push(b'\n');
    write
        .write_all(frame)
        .await
        .map_err(|error| format!("cannot write the answer: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client;
    use crate::drive::Settings;
    use crate::process::{Worker, WorkerExe};
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
        let store = Store::open(directory.path()).expect("the store opens");
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
