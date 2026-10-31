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

fn table_file(index: usize, size_bytes: u64) -> serde_json::Value {
    json!({
        "name": format!("catalogs/c/schemas/s/tables/t/files/{index}"),
        "size_bytes": size_bytes,
    })
}

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

#[test]
fn rate_one_keeps_every_file_and_sets_its_rate() {
    let input = format!(
        "{}\n{}\n",
        table_file(0, 1024 * 1024),
        table_file(1, 1024 * 1024)
    );
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
fn a_decimal_rate_keeps_about_that_fraction_and_sets_it() {
    let reference = 256 * 1024 * 1024;
    let input: String = (0..2_000)
        .map(|index| format!("{}\n", table_file(index, reference)))
        .collect();
    let output = pipe(&["sample", "files", "--rate", "0.01"], &input);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let kept = records(&output.stdout);
    assert!(
        (5..=60).contains(&kept.len()),
        "kept {} of 2000",
        kept.len()
    );
    for record in kept {
        let rate = record["sample_rate"].as_f64().expect("a sampled rate");
        assert!((rate - 0.01).abs() < 1e-12, "{rate}");
    }
}

#[test]
fn empty_input_is_silent() {
    let output = pipe(&["sample", "files", "--rate", "1"], "");
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}

#[test]
fn a_rate_outside_the_range_is_rejected() {
    for rate in ["0", "1/0", "1.5", "NaN", "inf", "-0.1"] {
        let output = pipe(&["sample", "files", "--rate", rate], "");
        assert!(!output.status.success(), "{rate}");
    }
}

#[test]
fn a_record_without_a_size_is_reported_and_the_rest_stream() {
    let input = format!(
        "{}\n{}\n",
        table_file(0, 1024 * 1024),
        json!({"name": "catalogs/c/schemas/s/tables/t/files/missing"})
    );
    let output = pipe(&["sample", "files", "--rate", "1"], &input);
    assert!(!output.status.success());
    assert_eq!(records(&output.stdout).len(), 1);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("has no positive size"), "{stderr}");
    assert!(stderr.contains("sampled 1 of 2 files"), "{stderr}");
}

#[test]
fn bytemass_carries_the_rate_onto_the_blob_and_columns() {
    let path = parquet_fixture();
    let size_bytes = std::fs::metadata(path).unwrap().len();
    let input = format!(
        "{}\n",
        json!({
            "name": "catalogs/local/schemas/local/tables/t/files/x",
            "blob": pqbench::v1::name::blob_name(path),
            "size_bytes": size_bytes,
            "sample_rate": 0.25,
        })
    );
    let output = pipe(&["bytemass"], &input);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let measured = records(&output.stdout);
    assert!(!measured.is_empty());
    for record in measured {
        assert_eq!(record["sample_rate"], json!(0.25), "{record}");
    }
}

#[test]
fn bytemass_rejects_an_invalid_rate() {
    let path = parquet_fixture();
    let size_bytes = std::fs::metadata(path).unwrap().len();
    let input = format!(
        "{}\n",
        json!({
            "name": "catalogs/local/schemas/local/tables/t/files/x",
            "blob": pqbench::v1::name::blob_name(path),
            "size_bytes": size_bytes,
            "sample_rate": 2.0,
        })
    );
    let output = pipe(&["bytemass"], &input);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("sample rate must be in (0, 1]"), "{stderr}");
}
