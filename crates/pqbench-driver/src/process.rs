//! The driver's worker commands: pqbench processes from the image the driver
//! opened before any of them started.
//!
//! A command execs `/proc/self/fd/{fd}` of the pinned binary rather than a
//! path, so a binary replaced on disk (an upgrade, a rebuild) changes no
//! command of a driver that is still running the old one.
//!
//! A child dies with the driver (`PR_SET_PDEATHSIG`) and with the task that
//! awaits it (`kill_on_drop`), so a restart finds none of its predecessor's
//! running, and no file a child was writing has a writer left.
//!
//! Every command is also bounded by the step timeout: a child that has not
//! finished by then is killed and reaped, so a hung worker — a wedged network
//! read, a page that will not decode — costs one step and leaves no process
//! behind. The engine then treats it as any other failure, and the version
//! retries with the usual backoff.

use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::time::timeout;

/// The most of a failed command's standard error kept as its error.
const ERROR_BYTES: usize = 2000;

/// The pqbench image the driver's worker commands run.
#[derive(Clone)]
pub(crate) struct WorkerExe {
    file: Arc<File>,
}

impl WorkerExe {
    /// Pin the pqbench binary: `explicit` when the operator named one, else
    /// the `pqbench` beside this executable, else the one on `PATH`.
    pub(crate) fn open(explicit: Option<&Path>) -> Result<Self, String> {
        let path = match explicit {
            Some(path) => path.to_path_buf(),
            None => default_binary()?,
        };
        let file = File::open(&path).map_err(|error| {
            format!("cannot open the pqbench binary {}: {error}", path.display())
        })?;
        Ok(WorkerExe {
            file: Arc::new(file),
        })
    }
}

/// The pqbench binary beside this executable, else the one on `PATH`.
fn default_binary() -> Result<PathBuf, String> {
    let exe = std::env::current_exe()
        .map_err(|error| format!("cannot find the pqbench-driver executable: {error}"))?;
    let sibling = exe.with_file_name("pqbench");
    if sibling.exists() {
        return Ok(sibling);
    }
    search_path().ok_or_else(|| {
        let searched = exe
            .parent()
            .map(|directory| directory.join("pqbench").display().to_string())
            .unwrap_or_default();
        format!(
            "cannot find the pqbench binary (looked for {searched} and on PATH): pass --pqbench"
        )
    })
}

fn search_path() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|directory| directory.join("pqbench"))
        .find(|candidate| candidate.is_file())
}

/// The pinned image, the daemon a command talks to, and how long a step may
/// run.
#[derive(Clone)]
pub(crate) struct Worker {
    exe: WorkerExe,
    daemon_socket: Option<PathBuf>,
    /// How long one command or walk may run before it is killed.
    step_timeout: Duration,
}

impl Worker {
    pub(crate) fn new(
        exe: WorkerExe,
        daemon_socket: Option<PathBuf>,
        step_timeout: Duration,
    ) -> Self {
        Worker {
            exe,
            daemon_socket,
            step_timeout,
        }
    }

    /// The pinned image as a command with `arguments`, talking to the daemon
    /// when there is one.
    fn command(&self, arguments: &[String]) -> Command {
        let image = format!("/proc/self/fd/{}", self.exe.file.as_raw_fd());
        let mut command = Command::new(image);
        command.args(arguments).arg0("pqbench").kill_on_drop(true);
        if let Some(socket) = &self.daemon_socket {
            command.env("PQB_DAEMON_SOCK", socket);
        }
        let parent = std::process::id() as libc::pid_t;
        // SAFETY: `prctl`, `getppid`, and `_exit` are async-signal-safe, so
        // they may run between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != parent {
                    libc::_exit(1);
                }
                Ok(())
            });
        }
        command
    }

    /// Run one command with `input` on its standard input: its standard
    /// output when it succeeds, else its error.
    pub(crate) async fn run(&self, arguments: &[String], input: &str) -> Result<Vec<u8>, String> {
        let name = arguments.join(" ");
        let mut child = self
            .command(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("cannot start `pqbench {name}`: {error}"))?;
        let mut stdin = child.stdin.take().expect("stdin is piped");
        let mut stdout = child.stdout.take().expect("stdout is piped");
        let mut stderr = child.stderr.take().expect("stderr is piped");
        // The whole input, then the pipe closes, so a command that reads to
        // end of file sees the end. A command that exits before reading its
        // input fails on its own.
        let write = async move {
            let _ = stdin.write_all(input.as_bytes()).await;
        };
        let collect = async {
            let mut out = Vec::new();
            let mut err = Vec::new();
            let (_, out_read, err_read, status) = tokio::join!(
                write,
                stdout.read_to_end(&mut out),
                stderr.read_to_end(&mut err),
                child.wait(),
            );
            out_read
                .and(err_read)
                .map_err(|error| format!("`pqbench {name}` was lost: {error}"))?;
            let status = status.map_err(|error| format!("`pqbench {name}` was lost: {error}"))?;
            Ok::<_, String>((out, err, status))
        };
        let (stdout, stderr, status) = match timeout(self.step_timeout, collect).await {
            Ok(collected) => collected?,
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(self.timed_out(&name));
            }
        };
        if status.success() {
            Ok(stdout)
        } else {
            Err(failure(&name, status, &stderr))
        }
    }

    /// The error a step that ran past [`Worker::step_timeout`] returns; the
    /// child is already killed and reaped.
    fn timed_out(&self, name: &str) -> String {
        format!(
            "`pqbench {name}` timed out after {:?} and was killed",
            self.step_timeout
        )
    }
}

/// Run a pipeline of commands, each one's standard output the next one's
/// standard input: the first reads `input`, the last writes `output`, all
/// write their errors to `error`. `Ok` when every stage succeeded.
pub(crate) async fn pipeline(
    worker: &Worker,
    stages: &[Vec<String>],
    input: File,
    output: File,
    error: &File,
) -> Result<(), String> {
    let mut children = Vec::new();
    let mut stdin = Stdio::from(input);
    let mut output = Some(output);
    for (at, arguments) in stages.iter().enumerate() {
        let stdout = if at + 1 == stages.len() {
            Stdio::from(output.take().expect("the last stage has the output"))
        } else {
            Stdio::piped()
        };
        let started = error.try_clone().and_then(|stderr| {
            worker
                .command(arguments)
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
    let failed = match timeout(worker.step_timeout, wait_children(&mut children)).await {
        Ok(failed) => failed,
        Err(_) => {
            kill_children(&mut children).await;
            return Err(format!(
                "the walk timed out after {:?} and was killed",
                worker.step_timeout
            ));
        }
    };
    if failed.is_empty() {
        Ok(())
    } else {
        Err(failed.join("; "))
    }
}

/// Await every stage, collecting how each ended.
async fn wait_children(children: &mut [(String, Child)]) -> Vec<String> {
    let mut failed = Vec::new();
    for (name, child) in children.iter_mut() {
        match child.wait().await {
            Ok(status) if status.success() => {}
            Ok(status) => failed.push(format!("`pqbench {name}` {status}")),
            Err(error) => failed.push(format!("`pqbench {name}` was lost: {error}")),
        }
    }
    failed
}

/// Kill and reap every stage of a pipeline that outlived its step.
async fn kill_children(children: &mut [(String, Child)]) {
    for (_, child) in children.iter_mut() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

/// A failed command's error: the end of its standard error, else how it
/// ended.
fn failure(name: &str, status: ExitStatus, stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let text = text.trim();
    if text.is_empty() {
        format!("`pqbench {name}` {status}")
    } else {
        tail(text)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A worker running the shell as its pinned image, bounded by a short
    /// step: the shell is a real command, not the pqbench binary, which the
    /// unit tests do not need to build.
    fn shell(step_timeout: Duration) -> Worker {
        let exe = WorkerExe::open(Some(Path::new("/bin/sh"))).expect("/bin/sh opens");
        Worker::new(exe, None, step_timeout)
    }

    /// A command that outlives its step is killed, so the rest of its script
    /// never runs, and the error names the timeout.
    #[tokio::test]
    async fn a_slow_command_is_killed_at_the_step_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("finished");
        let script = format!("sleep 0.3; touch {}", marker.display());
        let worker = shell(Duration::from_millis(100));
        let started = std::time::Instant::now();
        let error = worker
            .run(&["-c".to_string(), script], "")
            .await
            .expect_err("a command past its step is an error");
        assert!(error.contains("timed out"), "{error}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the step was not bounded: {:?}",
            started.elapsed()
        );
        assert!(!marker.exists(), "the killed step went on running");
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!marker.exists(), "the killed step went on running");
    }

    /// A command that finishes inside its step is untouched: its output is
    /// the result, and its error is its own.
    #[tokio::test]
    async fn a_command_inside_its_step_keeps_its_output() {
        let worker = shell(Duration::from_secs(30));
        let output = worker
            .run(&["-c".to_string(), "cat".to_string()], "hello\n")
            .await
            .unwrap();
        assert_eq!(output, b"hello\n");
        let error = worker
            .run(&["-c".to_string(), "echo boom >&2; exit 1".to_string()], "")
            .await
            .expect_err("a failing command is an error");
        assert_eq!(error, "boom");
    }

    /// A walk that outlives its step is killed stage by stage: the command
    /// that would land the listing never runs.
    #[tokio::test]
    async fn a_slow_walk_is_killed_at_the_step_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("finished");
        let script = format!("sleep 0.3; touch {}", marker.display());
        let input = tempfile::NamedTempFile::new().unwrap().into_file();
        let output = File::create(directory.path().join("walk.ndjson")).unwrap();
        let error = File::create(directory.path().join("walk.err")).unwrap();
        let worker = shell(Duration::from_millis(100));
        let staged = pipeline(
            &worker,
            &[vec!["-c".to_string(), script]],
            input,
            output,
            &error,
        )
        .await
        .expect_err("a walk past its step is an error");
        assert!(staged.contains("timed out"), "{staged}");
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!marker.exists(), "the killed walk went on running");
    }
}
