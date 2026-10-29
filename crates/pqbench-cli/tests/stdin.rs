//! Standard input kinds: a socket (a notebook kernel's stdin) is not a stream,
//! so a command must not read it and block; a redirected file is a stream.

use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

/// Wait for `child`, killing it and failing when it outlives the deadline: a
/// command that reads a stdin it should not blocks, and a block must fail.
fn wait_or_kill(mut child: Child, args: &[&str]) -> Output {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("{args:?} blocked reading standard input");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Run `pqbench` with stdin a connected socket whose peer stays open, so a read
/// would block instead of reaching end of input.
fn socket_stdin(args: &[&str]) -> Output {
    let (child_end, peer) = UnixStream::pair().unwrap();
    let child = pqbench()
        .args(args)
        .stdin(Stdio::from(OwnedFd::from(child_end)))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let output = wait_or_kill(child, args);
    drop(peer);
    output
}

#[test]
fn a_command_does_not_read_a_socket_standard_input() {
    for args in [
        &["list", "partitions"][..],
        &["list", "files"][..],
        &["bytemass"][..],
        &["diff", "-", "unused.parquet"][..],
    ] {
        let output = socket_stdin(args);
        assert!(
            !output.status.success(),
            "{args:?} succeeded on a socket standard input"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).starts_with("error: "),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
