//! Blackbox CLI tests: run the built `docscheck` over throwaway trees and
//! assert the files it writes and its freshness gate.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    run_in(Path::new("."), args)
}

fn run_in(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_docscheck"))
        .current_dir(dir)
        .args(args)
        .output()
        .expect("run docscheck")
}

fn write(dir: &Path, name: &str, source: &str) {
    let path = dir.join(name);
    fs::create_dir_all(path.parent().unwrap()).expect("mkdir");
    fs::write(path, source).expect("write fixture");
}

#[test]
fn sync_writes_a_test_file_and_support_module() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(temp.path(), "doc.md", "## lz\n\n```sh run\necho hi\n```\n");

    let output = run_in(temp.path(), &["sync", "doc.md", "--out", "gen"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let generated = fs::read_to_string(temp.path().join("gen/gen_doc.rs")).expect("gen_doc.rs");
    assert!(generated.contains("fn lz()"));
    assert!(temp.path().join("gen/gen_support.rs").exists());
}

#[test]
fn a_nested_path_becomes_an_underscored_file_name() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(temp.path(), "docs/demo.md", "```sh run\none\n```\n");

    run_in(temp.path(), &["sync", "docs", "--out", "gen"]);
    assert!(temp.path().join("gen/gen_docs_demo.rs").exists());
}

#[test]
fn check_passes_right_after_a_sync() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(temp.path(), "doc.md", "```sh run\necho hi\n```\n");

    run_in(temp.path(), &["sync", "doc.md", "--out", "gen"]);
    let output = run_in(temp.path(), &["check", "doc.md", "--out", "gen"]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
}

#[test]
fn check_fails_when_the_generated_file_is_missing() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(temp.path(), "doc.md", "```sh run\necho hi\n```\n");

    let output = run_in(temp.path(), &["check", "doc.md", "--out", "gen"]);
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8(output.stderr).expect("utf8");
    assert!(stderr.contains("out of date"), "stderr: {stderr}");
}

#[test]
fn check_fails_when_the_document_changed() {
    let temp = tempfile::tempdir().expect("tempdir");
    fs::write(temp.path().join("doc.md"), "```sh run\necho hi\n```\n").expect("write");
    run_in(temp.path(), &["sync", "doc.md", "--out", "gen"]);

    fs::write(temp.path().join("doc.md"), "```sh run\necho changed\n```\n").expect("rewrite");
    let output = run_in(temp.path(), &["check", "doc.md", "--out", "gen"]);
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn emit_prints_and_writes_nothing() {
    let temp = tempfile::tempdir().expect("tempdir");
    write(temp.path(), "doc.md", "## lz\n\n```sh run\necho hi\n```\n");

    let output = run_in(temp.path(), &["emit", "doc.md"]);
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    assert!(stdout.contains("fn lz()"));
}

#[test]
fn sync_removes_a_generated_file_whose_document_lost_its_run_blocks() {
    let temp = tempfile::tempdir().expect("tempdir");
    fs::write(temp.path().join("doc.md"), "```sh run\necho hi\n```\n").expect("write");
    run_in(temp.path(), &["sync", "doc.md", "--out", "gen"]);
    assert!(temp.path().join("gen/gen_doc.rs").exists());

    fs::write(temp.path().join("doc.md"), "```sh\necho hi\n```\n").expect("rewrite");
    run_in(temp.path(), &["sync", "doc.md", "--out", "gen"]);
    assert!(!temp.path().join("gen/gen_doc.rs").exists());
    assert!(temp.path().join("gen/gen_support.rs").exists());
}

#[test]
fn sync_leaves_hand_written_tests_alone() {
    let temp = tempfile::tempdir().expect("tempdir");
    fs::write(temp.path().join("doc.md"), "```sh run\necho hi\n```\n").expect("write");
    write(temp.path(), "gen/bytemass.rs", "// hand-written\n");
    run_in(temp.path(), &["sync", "doc.md", "--out", "gen"]);
    assert!(temp.path().join("gen/bytemass.rs").exists());
}

#[test]
fn fails_loudly_on_a_missing_path() {
    let output = run(&["sync", "/nonexistent/path/for/docscheck"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(!output.stderr.is_empty());
}
