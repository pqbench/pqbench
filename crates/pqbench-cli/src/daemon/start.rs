//! Starting, stopping, and checking the daemon.

use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use pqbench::Config;

use super::client;
use super::path;
use super::protocol::Request;

/// Start a background daemon for this identity, or reuse the one already
/// listening. With a `store` the daemon runs the master; one already running
/// without it is refused rather than reused.
pub(crate) async fn start(
    config: &Config,
    socket: &Path,
    idle_seconds: u64,
    store: Option<&Path>,
) -> Result<(), String> {
    if client::ping(socket).await {
        if store.is_some() && !runs_master(socket).await {
            return Err(format!(
                "a daemon without the master is running at {}; stop it first",
                socket.display()
            ));
        }
        return Ok(());
    }
    path::prepare(socket)?;

    let mut child = serve_command(config, socket, idle_seconds, store)?
        .spawn()
        .map_err(|error| format!("cannot start the daemon: {error}"))?;

    for _ in 0..250 {
        if client::ping(socket).await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    Err(format!("the daemon did not start on {}", socket.display()))
}

/// The background daemon's command: this executable, serving `socket` with
/// the resolved identity passed explicitly.
fn serve_command(
    config: &Config,
    socket: &Path,
    idle_seconds: u64,
    store: Option<&Path>,
) -> Result<Command, String> {
    let exe = std::env::current_exe()
        .map_err(|error| format!("cannot find the pqbench executable: {error}"))?;
    let mut command = Command::new(exe);
    command
        .arg("daemon")
        .arg("serve")
        .arg("--socket")
        .arg(socket)
        .arg("--idle-seconds")
        .arg(idle_seconds.to_string());
    if let Some(store) = store {
        command.arg("--master").arg("--store").arg(store);
    }
    // The child resolves the same identity from explicit environment values;
    // the token is never on its command line.
    for name in [
        "PQB_DAEMON_SOCK",
        "PQB_PROFILE",
        "PQB_TOKEN",
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
    ] {
        command.env_remove(name);
    }
    // The child gets the resolved identity explicitly; an empty config file
    // keeps a multi-profile ~/.pqbenchrc from changing it.
    command.env("PQB_CONFIG_FILE", "/dev/null");
    command.env("PQB_ENDPOINT", &config.endpoint);
    command.env("PQB_TABLE_FORMAT", config.dialect.as_str());
    if let Some(token) = &config.token {
        command.env("PQB_TOKEN", token);
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    // A new session keeps the daemon alive when the shell that started it
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

/// Whether the daemon at `socket` runs the master.
async fn runs_master(socket: &Path) -> bool {
    let request = Request::Partitions {
        table: Some(String::new()),
    };
    client::request(socket, &request)
        .await
        .is_ok_and(|response| response.error.is_none())
}

/// Stop the daemon listening at `socket`.
pub(crate) async fn stop(socket: &Path) -> Result<(), String> {
    match client::request(socket, &Request::stop()).await {
        Ok(_) => {}
        Err(client::Error::Connect(_)) => {
            return Err("the daemon is not running".into());
        }
        Err(error) => return Err(error.to_string()),
    }
    for _ in 0..100 {
        if !socket.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = std::fs::remove_file(socket);
    Ok(())
}

/// Report whether a daemon is listening at `socket`.
pub(crate) async fn status(socket: &Path) -> Result<(), String> {
    if client::ping(socket).await {
        println!("running {}", socket.display());
        Ok(())
    } else {
        Err("the daemon is not running".into())
    }
}
