use std::process::Command;

fn command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}
fn fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

#[test]
fn file_and_saved_stream_comparisons_agree() {
    let directory = tempfile::tempdir().unwrap();
    let saved = directory.path().join("mass.lz4");
    let measured = command()
        .args(["bytemass", fixture(), "-o"])
        .arg(&saved)
        .output()
        .unwrap();
    assert!(measured.status.success());
    let diff = command()
        .args(["diff", fixture()])
        .arg(&saved)
        .output()
        .unwrap();
    assert!(
        diff.status.success(),
        "{}",
        String::from_utf8_lossy(&diff.stderr)
    );
    let rows: Vec<serde_json::Value> = String::from_utf8(diff.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let columns: Vec<_> = rows
        .iter()
        .filter(|row| row["kind"] == "pqbench.diff-column")
        .collect();
    assert_eq!(columns.len(), 7);
    assert!(columns
        .iter()
        .all(|row| row["delta_bytes"] == 0 && row["left_rows"] == 3000));
    let wrong = directory.path().join("wrong.ndjson");
    std::fs::write(
        &wrong,
        "{\"kind\":\"pqbench.bytemass-page\",\"id\":\"t\"}\n",
    )
    .unwrap();
    let rejected = command()
        .args(["diff", fixture()])
        .arg(wrong)
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("bytemass stream"));
}
