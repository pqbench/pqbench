//! The daemon's child processes: pqbench commands started and checked
//! without waiting on them.
//!
//! A child dies with the daemon (`PR_SET_PDEATHSIG`), so a restart finds none
//! of its predecessor's running. The daemon is a child's parent, so a
//! non-blocking `waitpid` reaps it and reads its exit status; nothing is held
//! while a command runs.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};

/// Where a child is.
pub(crate) enum Exit {
    Running,
    /// It exited: `Ok` on success, else how it ended.
    Exited(Result<(), String>),
}

/// The pqbench command `arguments` from the daemon's own executable, talking
/// to the daemon on `socket`. `/proc/self/exe` is the image the daemon runs,
/// so a binary replaced on disk (an upgrade, a rebuild) changes no command
/// of a daemon that is still running the old one.
fn command(arguments: &[String], socket: &Path) -> io::Result<Command> {
    let mut command = Command::new("/proc/self/exe");
    command.args(arguments).env("PQB_DAEMON_SOCK", socket);
    // SAFETY: `prctl` is async-signal-safe, so it may run between fork and
    // exec.
    unsafe {
        command.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(command)
}

/// Start one command: standard input from `input`, standard output to
/// `output` (discarded when `None`), standard error appended to `error`.
/// Returns its process id.
pub(crate) fn start(
    arguments: &[String],
    input: &Path,
    output: Option<&Path>,
    error: &Path,
    socket: &Path,
) -> io::Result<u32> {
    let stdout = match output {
        Some(path) => Stdio::from(File::create(path)?),
        None => Stdio::null(),
    };
    // Appended: a walk's phase writes several commands into one error file.
    let error = OpenOptions::new().create(true).append(true).open(error)?;
    let child = command(arguments, socket)?
        .stdin(File::open(input)?)
        .stdout(stdout)
        .stderr(error)
        .spawn()?;
    Ok(child.id())
}

/// Start a pipeline of commands, each one's standard output the next one's
/// standard input: the first reads `input`, the last writes `output`, all
/// write their errors to `error`. Returns their process ids.
pub(crate) fn start_pipeline(
    stages: &[Vec<String>],
    input: File,
    output: File,
    error: &File,
    socket: &Path,
) -> io::Result<Vec<u32>> {
    let mut process_ids = Vec::new();
    let mut stdin = Stdio::from(input);
    let mut output = Some(output);
    for (at, arguments) in stages.iter().enumerate() {
        let last = at + 1 == stages.len();
        let stdout = match (last, output.take()) {
            (true, Some(file)) => Stdio::from(file),
            _ => Stdio::piped(),
        };
        let mut child = command(arguments, socket)?
            .stdin(stdin)
            .stdout(stdout)
            .stderr(error.try_clone()?)
            .spawn()?;
        process_ids.push(child.id());
        stdin = match child.stdout.take() {
            Some(pipe) => Stdio::from(pipe),
            None => Stdio::null(),
        };
    }
    Ok(process_ids)
}

/// Whether the child `process_id` has exited, without waiting: reaping it
/// reads its status.
pub(crate) fn poll(process_id: u32) -> Exit {
    let mut status = 0;
    // SAFETY: waitpid writes the status into a local; WNOHANG never blocks.
    let reaped = unsafe { libc::waitpid(process_id as i32, &mut status, libc::WNOHANG) };
    match reaped {
        0 => Exit::Running,
        -1 => Exit::Exited(Err(format!(
            "the process was lost: {}",
            io::Error::last_os_error()
        ))),
        _ => {
            let status = ExitStatus::from_raw(status);
            Exit::Exited(if status.success() {
                Ok(())
            } else {
                Err(status.to_string())
            })
        }
    }
}

/// Kill the child `process_id`; the next poll reaps it.
pub(crate) fn kill(process_id: u32) {
    // SAFETY: a signal to a child this daemon started and has not reaped, so
    // the id still names it.
    unsafe {
        libc::kill(process_id as i32, libc::SIGKILL);
    }
}
