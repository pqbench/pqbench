use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::json;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn pipe(args: &[&str], stdin: &str) -> Output {
    let mut child = pqbench()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

fn table_file(index: usize) -> serde_json::Value {
    json!({
        "name": format!("catalogs/c/schemas/s/tables/t/files/{index}"),
        "size_bytes": 1024 * 1024,
    })
}

#[test]
fn rate_one_keeps_every_file_and_sets_its_rate() {
    let input = format!("{}\n{}\n", table_file(0), table_file(1));
    let output = pipe(&["sample", "files", "--rate", "1"], &input);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let kept = records(&output.stdout);
    assert_eq!(kept.len(), 2);
    for record in kept {
        assert_eq!(record["sample_rate"], json!(1.0));
    }
    assert!(String::from_utf8_lossy(&output.stderr).contains("sampled 2 of 2 files"));
}

#[test]
fn a_fraction_rate_is_accepted() {
    let output = pipe(&["sample", "files", "--rate", "1/100"], "");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn a_rate_outside_the_range_is_rejected() {
    let output = pipe(&["sample", "files", "--rate", "0"], "");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("outside (0, 1]"), "{stderr}");
}

#[test]
fn a_record_without_a_size_is_reported() {
    let input = format!(
        "{}\n",
        json!({"name": "catalogs/c/schemas/s/tables/t/files/a"})
    );
    let output = pipe(&["sample", "files", "--rate", "1"], &input);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("needs a positive file size"), "{stderr}");
}
