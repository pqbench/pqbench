//! Blackbox tests for `pqbench credentials get`: the shell env `--shell-env`
//! writes for one ref, offline through the Iceberg dialect (which never vends).

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

/// `--shell-env` writes one ref's env as shell assignments for a loop's `eval`:
/// the lake source's options first, the ref's over them. The Iceberg dialect
/// never vends, so this runs offline.
#[test]
fn credentials_get_shell_env_writes_shell_assignments() {
    let source = concat!(
        r#"{"kind":"pqbench.lake-source","version":1,"table_format":"iceberg","endpoint":"http://example.test","env":{"AWS_REGION":"us-east-1","AWS_ENDPOINT":"http://minio.test:9000"}}"#,
        "\n"
    );
    let reference = concat!(
        r#"{"kind":"pqbench.table-ref","version":2,"id":"dbx_samples.nyctaxi.trips","uri":"http://x/tables/dbx_samples.nyctaxi.trips","env":{"AWS_REGION":"eu-west-1"}}"#,
        "\n"
    );
    let output = pipe(
        &["credentials", "get", "--shell-env"],
        format!("{source}{reference}").as_bytes(),
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "export AWS_ENDPOINT='http://minio.test:9000'\nexport AWS_REGION='eu-west-1'\n"
    );
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
