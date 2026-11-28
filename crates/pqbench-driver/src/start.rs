//! Starting, stopping, and checking pqbench-driver.

use std::fs::OpenOptions;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use super::client;
use super::protocol::Request;
use super::socket;
use super::{RunArgs, TargetArgs};

/// Start a background driver, or reuse the one already listening.
pub(crate) async fn start(args: &RunArgs) -> Result<(), String> {
    let socket = socket::resolve(args.socket.as_deref(), Some(&args.store()))?;
    if client::ping(&socket).await {
        return Ok(());
    }
    socket::prepare(&socket, "driver")?;

    let mut child = serve_command(args, &socket)?
        .spawn()
        .map_err(|error| format!("cannot start pqbench-driver: {error}"))?;

    for _ in 0..250 {
        if client::ping(&socket).await {
            return Ok(());
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("cannot check the driver: {error}"))?
        {
            return Err(format!(
                "pqbench-driver exited ({status}) before serving {}; see {}",
                socket.display(),
                log_path(&socket).display()
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    Err(format!(
        "pqbench-driver did not start on {}",
        socket.display()
    ))
}

/// The background driver's command: this executable, serving `socket` with
/// the operator's flags passed explicitly.
fn serve_command(args: &RunArgs, socket: &Path) -> Result<Command, String> {
    let exe = std::env::current_exe()
        .map_err(|error| format!("cannot find the pqbench-driver executable: {error}"))?;
    let mut command = Command::new(exe);
    command
        .arg("serve")
        .arg("--store")
        .arg(args.store())
        .arg("--socket")
        .arg(socket);
    if let Some(daemon_socket) = &args.daemon_socket {
        command
            .arg("--daemon-socket")
            .arg(socket::absolute(daemon_socket));
    }
    if let Some(pqbench) = &args.pqbench {
        command.arg("--pqbench").arg(socket::absolute(pqbench));
    }
    if let Some(tables) = &args.walk_tables {
        command.arg("--walk-tables").arg(socket::absolute(tables));
    }
    if let Some(seconds) = args.walk_period_seconds {
        command
            .arg("--walk-period-seconds")
            .arg(seconds.to_string());
    }
    if args.low_network {
        command.arg("--low-network");
    }
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path(socket))
        .map_err(|error| format!("cannot open the driver log: {error}"))?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    // A new session keeps the driver alive when the shell that started it
    // closes the terminal.
    // SAFETY: `pre_exec` runs after fork and before exec, so the closure may
    // only call async-signal-safe functions; `setsid` is one.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    Ok(command)
}

/// The driver's log: beside its socket, so a failure is written down where
/// the error that reports it can point.
fn log_path(socket: &Path) -> PathBuf {
    socket.with_extension("log")
}

/// Stop the driver listening at `socket`.
pub(crate) async fn stop(args: &TargetArgs) -> Result<(), String> {
    let socket = socket::resolve(args.socket.as_deref(), args.store.as_deref())?;
    match client::request(&socket, &Request::stop()).await {
        Ok(_) => {}
        Err(client::Error::Connect(_)) => {
            return Err("pqbench-driver is not running".into());
        }
        Err(error) => return Err(error.to_string()),
    }
    // The stop answers before the driver has checkpointed the map and removed
    // its socket: a large map's checkpoint is the slow part, so wait for it
    // rather than unlinking a socket a live driver still owns.
    for _ in 0..100 {
        if !socket.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(format!(
        "pqbench-driver did not stop within 5 s: {} is still there",
        socket.display()
    ))
}

/// Report the driver's state as one JSON object: the versions by state, what
/// it is running, and how its walks have gone.
pub(crate) async fn status(args: &TargetArgs) -> Result<(), String> {
    let socket = socket::resolve(args.socket.as_deref(), args.store.as_deref())?;
    let response = match client::request(&socket, &Request::status()).await {
        Ok(response) => response,
        Err(client::Error::Connect(_)) => return Err("pqbench-driver is not running".into()),
        Err(error) => return Err(error.to_string()),
    };
    if let Some(error) = response.error {
        return Err(error);
    }
    let Some(status) = response.status else {
        return Err("the driver answered no status".into());
    };
    let line = serde_json::to_string(&status).map_err(|error| error.to_string())?;
    println!("{line}");
    Ok(())
}

/// List the suggestions the driver measured under `parent` (all when it is
/// absent), one Suggestion per line, largest saving first.
pub(crate) async fn list_suggestions(parent: Option<&str>) -> Result<(), String> {
    let socket =
        socket::from_env().ok_or_else(|| format!("no driver socket: set {}", socket::ENV))?;
    let request = Request::list_suggestions(parent.map(ToString::to_string));
    let response = match client::request(&socket, &request).await {
        Ok(response) => response,
        Err(client::Error::Connect(_)) => {
            return Err(format!("no driver is listening at {}", socket.display()));
        }
        Err(error) => return Err(error.to_string()),
    };
    if let Some(error) = response.error {
        return Err(error);
    }
    for record in &response.records {
        // The record is the JSON the driver read: print it as it is.
        println!("{}", record.get());
    }
    Ok(())
}
