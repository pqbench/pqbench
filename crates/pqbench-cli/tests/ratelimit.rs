//! `ratelimit` blackbox: records pass through unchanged, delayed per kind.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn run(args: &[&str], stdin: &[u8]) -> std::process::Output {
    let mut child = pqbench()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    child.wait_with_output().unwrap()
}

/// Run `ratelimit` and time each output line's arrival.
fn arrivals(args: &[&str], stdin: &[u8]) -> (std::process::ExitStatus, Vec<Duration>, Vec<String>) {
    let mut child = pqbench()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    let stdout = child.stdout.take().unwrap();
    let start = Instant::now();
    let mut times = Vec::new();
    let mut lines = Vec::new();
    for line in BufReader::new(stdout).lines() {
        let line = line.unwrap();
        times.push(start.elapsed());
        lines.push(line);
    }
    (child.wait().unwrap(), times, lines)
}

const CATALOG: &str = r#"{"kind":"pqbench.catalog","version":1,"name":"dbx_samples"}"#;
const SCHEMA: &str =
    r#"{"kind":"pqbench.schema","version":1,"catalog":"dbx_samples","name":"nyctaxi"}"#;

#[test]
fn passes_records_through_unchanged() {
    let input = format!("{CATALOG}\n{SCHEMA}\n");
    let output = run(&["ratelimit", "--rate", "0"], input.as_bytes());
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap(), input);
}

#[test]
fn paces_one_kind() {
    let input = format!("{CATALOG}\n{CATALOG}\n{CATALOG}\n");
    let (status, times, lines) = arrivals(&["ratelimit", "--rate", "5"], input.as_bytes());
    assert!(status.success());
    assert_eq!(lines.len(), 3);
    assert!(
        times[1] - times[0] >= Duration::from_millis(120),
        "{times:?}"
    );
    assert!(
        times[2] - times[1] >= Duration::from_millis(120),
        "{times:?}"
    );
}

#[test]
fn paces_kinds_independently() {
    let input = format!("{CATALOG}\n{SCHEMA}\n{CATALOG}\n");
    let (status, times, lines) = arrivals(&["ratelimit", "--rate", "5"], input.as_bytes());
    assert!(status.success());
    assert_eq!(lines.len(), 3);
    assert!(
        times[1] - times[0] < Duration::from_millis(150),
        "different kinds should not wait for each other: {times:?}"
    );
    assert!(
        times[2] - times[0] >= Duration::from_millis(150),
        "the second catalog should wait a slot: {times:?}"
    );
}

#[test]
fn rejects_a_bad_record() {
    let output = run(&["ratelimit", "--rate", "0"], b"not json\n");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("NDJSON"), "{stderr}");
}

#[test]
fn rejects_a_bad_rate() {
    let output = run(&["ratelimit", "--rate=-1"], b"");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("non-negative"), "{stderr}");
}
