//! The daemon's child processes: pqbench commands from the daemon's own
//! executable, awaited on the daemon's runtime.
//!
//! A child dies with the daemon (`PR_SET_PDEATHSIG`) and with the task that
//! awaits it (`kill_on_drop`), so a restart finds none of its predecessor's
//! running, and no file a child was writing has a writer left.

use std::fs::File;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

/// The most of a failed command's standard error kept as its error.
const ERROR_BYTES: usize = 2000;

/// The pqbench command `arguments`, talking to the daemon on `socket`.
/// `/proc/self/exe` is the image the daemon runs, so a binary replaced on
/// disk (an upgrade, a rebuild) changes no command of a daemon that is still
/// running the old one.
fn command(arguments: &[String], socket: &Path) -> Command {
    let mut command = Command::new("/proc/self/exe");
    command
        .args(arguments)
        .env("PQB_DAEMON_SOCK", socket)
        .kill_on_drop(true);
    // SAFETY: `prctl` is async-signal-safe, so it may run between fork and
    // exec.
    unsafe {
        command.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command
}

/// Run one command with `input` on its standard input: its standard output
/// when it succeeds, else its error.
pub(crate) async fn run(
    arguments: &[String],
    input: &str,
    socket: &Path,
) -> Result<Vec<u8>, String> {
    let name = arguments.join(" ");
    let mut child = command(arguments, socket)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("cannot start `pqbench {name}`: {error}"))?;
    let mut stdin = child.stdin.take().expect("stdin is piped");
    let write = async move {
        // A command that exits before reading its input fails on its own.
        let _ = stdin.write_all(input.as_bytes()).await;
    };
    let (_, output) = tokio::join!(write, child.wait_with_output());
    let output = output.map_err(|error| format!("`pqbench {name}` was lost: {error}"))?;
    match output.status.success() {
        true => Ok(output.stdout),
        false => Err(failure(&name, output.status, &output.stderr)),
    }
}

/// Run a pipeline of commands, each one's standard output the next one's
/// standard input: the first reads `input`, the last writes `output`, all
/// write their errors to `error`. `Ok` when every stage succeeded.
pub(crate) async fn pipeline(
    stages: &[Vec<String>],
    input: File,
    output: File,
    error: &File,
    socket: &Path,
) -> Result<(), String> {
    let mut children = Vec::new();
    let mut stdin = Stdio::from(input);
    let mut output = Some(output);
    for (at, arguments) in stages.iter().enumerate() {
        let stdout = match (at + 1 == stages.len(), output.take()) {
            (true, Some(file)) => Stdio::from(file),
            _ => Stdio::piped(),
        };
        let started = error.try_clone().and_then(|stderr| {
            command(arguments, socket)
                .stdin(stdin)
                .stdout(stdout)
                .stderr(stderr)
                .spawn()
        });
        let mut child = started
            .map_err(|error| format!("cannot start `pqbench {}`: {error}", arguments.join(" ")))?;
        stdin = match child.stdout.take() {
            Some(pipe) => pipe.try_into().map_err(|error| format!("{error}"))?,
            None => Stdio::null(),
        };
        children.push((arguments.join(" "), child));
    }
    let mut failed = Vec::new();
    for (name, mut child) in children {
        match child.wait().await {
            Ok(status) if status.success() => {}
            Ok(status) => failed.push(format!("`pqbench {name}` {status}")),
            Err(error) => failed.push(format!("`pqbench {name}` was lost: {error}")),
        }
    }
    match failed.is_empty() {
        true => Ok(()),
        false => Err(failed.join("; ")),
    }
}

/// A failed command's error: the end of its standard error, else how it
/// ended.
fn failure(name: &str, status: ExitStatus, stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let text = text.trim();
    match text.is_empty() {
        true => format!("`pqbench {name}` {status}"),
        false => tail(text),
    }
}

/// The last [`ERROR_BYTES`] of `text`.
pub(crate) fn tail(text: &str) -> String {
    if text.len() <= ERROR_BYTES {
        return text.to_string();
    }
    let mut start = text.len() - ERROR_BYTES;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    format!("…{}", &text[start..])
}
