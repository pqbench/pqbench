//! Blackbox tests for `pqbench credentials get`: the `--shell-env` one-ref
//! rule, which fails before any request. The vend paths themselves run against
//! the live Databricks suite and the lakehouse stand, not here.

use std::io::Write;
use std::process::{Command, Stdio};

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn pipe(args: &[&str], stdin: &[u8]) -> std::process::Output {
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

/// `--shell-env` populates one table's env: a second ref fails before any output.
#[test]
fn credentials_get_shell_env_takes_one_ref() {
    let source = r#"{"kind":"pqbench.lake-source","version":1,"table_format":"iceberg","endpoint":"http://example.test"}"#;
    let reference = r#"{"kind":"pqbench.table-ref","version":2,"id":"dbx_samples.nyctaxi.trips","uri":"http://x/tables/dbx_samples.nyctaxi.trips"}"#;
    let output = pipe(
        &["credentials", "get", "--shell-env"],
        format!("{source}\n{reference}\n{reference}\n").as_bytes(),
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("one ref"), "{stderr}");
}
