use std::process::Command;

/// End-to-end: run the `pqbench bytemass` CLI and verify the text (tui) stats
/// it prints for a real parquet file.
#[test]
fn bytemass_text_stats_end_to_end() {
    let exe = env!("CARGO_BIN_EXE_pqbench");
    let file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../data/bench/reddit_chunk1_NONE.parquet"
    );
    let out = Command::new(exe).args(["bytemass", file]).output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).unwrap();

    assert!(stdout.contains("bytemass: reddit_chunk1_NONE.parquet"));
    assert!(stdout.contains("column"));
    assert!(stdout.contains("bytes/row"));

    for column in [
        "url_encoded",
        "text",
        "username_encoded",
        "label",
        "communityName",
        "dataType",
        "datetime",
    ] {
        assert!(stdout.contains(column), "missing column {column}");
    }

    // Sorted descending by bytes/row: url_encoded > text > username_encoded.
    let url = stdout.find("url_encoded").unwrap();
    let text = stdout.find("text").unwrap();
    let username = stdout.find("username_encoded").unwrap();
    assert!(url < text && text < username, "columns not sorted by value");

    assert!(stdout.lines().any(|l| l.trim_start().starts_with("total")));
}
