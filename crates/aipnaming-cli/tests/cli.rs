//! Blackbox CLI tests: run the built binary over throwaway trees and assert
//! its output contract, exit codes, and JSON shape.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_aipnaming"))
        .args(args)
        .output()
        .expect("run aipnaming")
}

fn write(dir: &Path, name: &str, source: &str) {
    fs::write(dir.join(name), source).expect("write fixture");
}

#[test]
fn reports_findings_with_positions_and_exits_one() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(
        temp.path(),
        "page.rs",
        "pub struct Page {\n    pub is_dictionary: bool,\n}\n",
    );

    let output = run(&[temp.path().to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    assert!(
        stdout.contains("page.rs:2:9: error[aip-140/booleans]"),
        "stdout was: {stdout}"
    );
}

#[test]
fn stays_silent_and_exits_zero_on_clean_input() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(
        temp.path(),
        "page.rs",
        "/// One page.\npub struct Page {\n    pub dictionary: bool,\n}\n",
    );

    let output = run(&[temp.path().to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}

#[test]
fn json_output_is_one_object_per_finding() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(
        temp.path(),
        "page.rs",
        "pub struct Page {\n    pub is_dictionary: bool,\n}\n",
    );

    let output = run(&["--json", temp.path().to_str().unwrap()]);
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    let lines: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("json line"))
        .collect();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["rule"], "aip-140/booleans");
    assert_eq!(lines[0]["line"], 2);
    assert_eq!(lines[0]["help"], "dictionary");
}

#[test]
fn github_format_emits_annotations() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(
        temp.path(),
        "page.rs",
        "pub struct Page {\n    pub is_dictionary: bool,\n}\n",
    );

    let output = run(&["--output-format", "github", temp.path().to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    assert!(
        stdout.contains("::error file=") && stdout.contains("line=2,col=9"),
        "stdout was: {stdout}"
    );
    assert!(
        stdout.contains("title=aipnaming aip-140/booleans"),
        "stdout was: {stdout}"
    );
}

#[test]
fn statistics_summarize_by_rule() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(
        temp.path(),
        "page.rs",
        "pub struct Page {\n    pub is_dictionary: bool,\n    pub is_json: bool,\n}\n",
    );

    let output = run(&["--statistics", temp.path().to_str().unwrap()]);
    let stderr = String::from_utf8(output.stderr).expect("utf8");
    assert!(stderr.contains("2 finding(s)"), "stderr was: {stderr}");
    assert!(
        stderr.contains("2  aip-140/booleans"),
        "stderr was: {stderr}"
    );
}

#[test]
fn exit_zero_suppresses_the_failure_code() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(
        temp.path(),
        "page.rs",
        "pub struct Page {\n    pub is_dictionary: bool,\n}\n",
    );

    let output = run(&["--exit-zero", temp.path().to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));
    assert!(!output.stdout.is_empty(), "findings still print");
}

#[test]
fn lists_rules_without_linting() {
    let output = run(&["--list-rules"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    assert!(stdout.contains("aip-140/booleans"));
    assert!(!stdout.contains("aip-126/unspecified"));
}

#[test]
fn fails_loudly_on_a_missing_path() {
    let output = run(&["/nonexistent/path/for/aipnaming"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(!output.stderr.is_empty());
}

#[test]
fn skips_hidden_and_target_directories() {
    let temp = tempfile::tempdir().expect("tempdir");
    fs::create_dir(temp.path().join("target")).expect("target dir");
    fs::create_dir(temp.path().join(".git")).expect("git dir");
    write(
        &temp.path().join("target"),
        "gen.rs",
        "pub struct Gen { pub is_dictionary: bool }\n",
    );
    write(
        &temp.path().join(".git"),
        "hook.rs",
        "pub struct Hook { pub is_dictionary: bool }\n",
    );

    let output = run(&[temp.path().to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stdout.is_empty());
}
