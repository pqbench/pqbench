//! The commands the docs promise, run for real.
//!
//! Each test drives the built `pqbench` binary against the committed fixtures
//! and asserts the documented shape of the output. It is the executable half of
//! README.md, docs/*.md, and skills/*.md: if a documented command stops working,
//! this fails.
//!
//! Commands that need `aws` (`s3://`) or a live catalog (`make lakehouse`) are
//! not here: the default test build has no `aws`, so those are covered by the
//! lakehouse stand and the `aws` CI matrix.

use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(feature = "delta")]
use std::process::Stdio;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

/// The repository root, three levels up from `crates/pqbench-cli/`.
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .unwrap()
}

fn sample() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/small_reddit_none.parquet")
}

#[cfg(feature = "delta")]
fn delta_table() -> PathBuf {
    root().join("docker/e2e-lakehouse/table")
}

fn run(args: &[&str]) -> std::process::Output {
    pqbench().args(args).output().unwrap()
}

/// Run `pqbench <args>` and assert success, returning stdout.
fn run_ok(args: &[&str]) -> String {
    let output = run(args);
    assert!(
        output.status.success(),
        "`pqbench {}` failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn ndjson(stdout: &str) -> Vec<serde_json::Value> {
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("ndjson line"))
        .collect()
}

#[test]
fn readme_bytemass_reads_a_file() {
    let stdout = run_ok(&["bytemass", sample().to_str().unwrap(), "--json"]);
    let records = ndjson(&stdout);
    assert!(records
        .iter()
        .any(|record| record["kind"] == "pqbench.bytemass-row"));
    assert!(records.iter().any(|record| record["event"] == "end"));
}

#[test]
fn readme_bytemass_aggregates_a_glob() {
    // Two committed small fixtures live in the core crate's tree.
    let pattern = format!(
        "{}/crates/pqbench/tests/fixtures/small_*.parquet",
        root().display()
    );
    let stdout = run_ok(&["bytemass", &pattern, "--json"]);
    let end = ndjson(&stdout)
        .into_iter()
        .find(|record| record["event"] == "end")
        .unwrap();
    assert_eq!(end["file_count"], 2);
}

#[test]
fn readme_lz_sweeps_raw_bytes() {
    let readme = root().join("README.md");
    let stdout = run_ok(&[
        "lz",
        readme.to_str().unwrap(),
        "-c",
        "zstd@3",
        "--samples",
        "1",
        "--warmup-iterations",
        "0",
    ]);
    assert!(stdout.contains("zstd"));
}

#[test]
fn readme_compression_sweeps_parquet_pages() {
    let stdout = run_ok(&[
        "compression",
        sample().to_str().unwrap(),
        "--samples",
        "1",
        "--warmup-iterations",
        "0",
    ]);
    for codec in ["snappy", "zstd", "gzip", "lz4"] {
        assert!(stdout.contains(codec), "compression report missing {codec}");
    }
}

#[cfg(feature = "delta")]
#[test]
fn readme_table_document_pipes_to_bytemass() {
    let stdout = run_ok(&["table", delta_table().to_str().unwrap()]);
    let records = ndjson(&stdout);
    assert!(records
        .iter()
        .any(|record| record["kind"] == "pqbench.table"));
    let dir = root().join("target");
    std::fs::create_dir_all(&dir).unwrap();
    let document = dir.join("docs-table.ndjson");
    std::fs::write(&document, &stdout).unwrap();
    let piped = run_ok(&["bytemass", document.to_str().unwrap(), "--json"]);
    assert!(ndjson(&piped)
        .iter()
        .any(|record| record["kind"] == "pqbench.bytemass-row"));
    std::fs::remove_file(&document).ok();
}

#[cfg(feature = "delta")]
#[test]
fn readme_lake_walks_a_directory_into_table_refs() {
    let stdout = run_ok(&[
        "lake",
        root().join("docker/e2e-lakehouse").to_str().unwrap(),
    ]);
    let records = ndjson(&stdout);
    assert!(records
        .iter()
        .any(|record| record["kind"] == "pqbench.table-ref"));
}

#[test]
fn cli_profile_emits_column_facts() {
    let stdout = run_ok(&[
        "profile",
        sample().to_str().unwrap(),
        "--columns",
        "text",
        "--top",
        "5",
    ]);
    let columns: Vec<_> = ndjson(&stdout)
        .into_iter()
        .filter(|record| record["kind"] == "pqbench.profile-column")
        .collect();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["column"], "text");
    assert!(columns[0]["ndv"].is_number());
}

#[test]
fn cli_experiment_rewrites_and_measures() {
    let stdout = run_ok(&[
        "experiment",
        sample().to_str().unwrap(),
        "--rewrite",
        "sort:text",
        "--aim",
        "all",
    ]);
    let trials: Vec<_> = ndjson(&stdout)
        .into_iter()
        .filter(|record| record["kind"] == "pqbench.experiment-trial")
        .collect();
    assert!(trials.iter().any(|trial| trial["name"] == "control"));
    assert!(trials.iter().any(|trial| trial["name"] == "sort:text"));
}

#[test]
fn cli_skill_prints_the_bundled_advisor() {
    let listed = run_ok(&["skill"]);
    assert!(ndjson(&listed)
        .iter()
        .any(|record| record["kind"] == "pqbench.skill"));
    let body = run_ok(&["skill", "parquet-advisor"]);
    assert!(body.contains("pqbench experiment"));
    let recipes = run_ok(&["skill", "parquet-advisor", "recipes"]);
    assert!(recipes.contains("WRITE ORDERED BY"));
}

#[test]
fn cli_help_lists_every_documented_command() {
    let help = run_ok(&["--help"]);
    for command in [
        "lz",
        "compression",
        "bytemass",
        "table",
        "lake",
        "dump",
        "profile",
        "experiment",
        "skill",
        "viz",
    ] {
        assert!(help.contains(command), "--help does not list {command}");
    }
}

#[cfg(feature = "delta")]
#[test]
fn docs_viz_chains_table_into_html() {
    let output = root().join("target/docs-viz");
    // table | bytemass | viz -o <prefix>
    let mut table = pqbench()
        .args(["table", delta_table().to_str().unwrap()])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut mass = pqbench()
        .args(["bytemass", "-", "--json"])
        .stdin(table.stdout.take().unwrap())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    table.wait().unwrap();
    let mut viz = pqbench()
        .args(["viz", "-o", output.to_str().unwrap()])
        .stdin(mass.stdout.take().unwrap())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    mass.wait().unwrap();
    assert!(viz.wait().unwrap().success());
    let html = output.with_extension("html");
    assert!(html.exists(), "viz did not write {}", html.display());
    std::fs::remove_file(&html).ok();
}

#[cfg(feature = "delta")]
#[test]
fn docs_dump_copies_a_table() {
    let out = root().join("target/docs-dump");
    let _ = std::fs::remove_dir_all(&out);
    run_ok(&[
        "dump",
        out.to_str().unwrap(),
        delta_table().to_str().unwrap(),
    ]);
    assert!(out.exists(), "dump did not create {}", out.display());
    std::fs::remove_dir_all(&out).ok();
}
