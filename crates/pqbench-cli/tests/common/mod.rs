//! Helpers shared by the CLI's blackbox tests.

use std::io::Write as _;
use std::process::{Command, Output, Stdio};

/// The table-file document a local Parquet path names, under `local.local`.
pub(crate) fn table_file(path: &str) -> String {
    let file = path
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(path);
    format!(
        "{{\"name\":\"{}\",\"blob\":\"{}\"}}\n",
        pqbench::v1::name::table_file_name(&pqbench::v1::name::local_table(path), file),
        pqbench::v1::name::blob_name(path)
    )
}

/// A `pqbench bytemass` process reading one file's table-file record.
pub(crate) fn measure(path: &str) -> std::process::Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .arg("bytemass")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(table_file(path).as_bytes())
        .unwrap();
    child
}

/// Measure one file and wait for its records.
pub(crate) fn measured(path: &str) -> Output {
    measure(path).wait_with_output().unwrap()
}

/// A `pqbench daemon serve` for one test command, on a private socket and
/// with the command's own environment; it stops when dropped.
pub(crate) struct TestDaemon {
    child: Option<std::process::Child>,
    _directory: tempfile::TempDir,
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Start the daemon a catalog command needs and point `command` at it. A
/// command whose environment names no endpoint (a local read, or a test of the
/// missing endpoint) gets none.
pub(crate) fn serve_for(command: &mut Command) -> TestDaemon {
    let directory = tempfile::tempdir().unwrap();
    let names_endpoint = command
        .get_envs()
        .any(|(key, value)| key == "PQB_ENDPOINT" && value.is_some_and(|value| !value.is_empty()));
    if !names_endpoint {
        return TestDaemon {
            child: None,
            _directory: directory,
        };
    }
    let socket = directory.path().join("d.sock");
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_pqbench"));
    daemon
        .args(["daemon", "serve", "--socket"])
        .arg(&socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in command.get_envs() {
        match value {
            Some(value) => daemon.env(key, value),
            None => daemon.env_remove(key),
        };
    }
    let mut child = daemon.spawn().unwrap();
    for _ in 0..500 {
        if socket.exists() || child.try_wait().ok().flatten().is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    command.env("PQB_DAEMON_SOCK", &socket);
    TestDaemon {
        child: Some(child),
        _directory: directory,
    }
}
