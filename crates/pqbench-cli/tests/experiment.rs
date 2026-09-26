use std::process::Command;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn parquet_fixture() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/small_reddit_none.parquet"
    )
}

fn ndjson(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

#[test]
fn experiment_streams_a_control_and_a_sort_trial() {
    let output = pqbench()
        .args([
            "experiment",
            parquet_fixture(),
            "--rows",
            "first:1024",
            "--rewrite",
            "sort:text",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records[0]["kind"], "pqbench.experiment");
    assert_eq!(records[0]["event"], "begin");
    assert_eq!(records[0]["aim"], "storage");
    let capabilities = records[0]["capabilities"].as_array().unwrap();
    assert!(!capabilities.is_empty());
    assert!(capabilities.iter().any(|capability| {
        capability["returns"]
            .as_array()
            .is_some_and(|returns| returns.iter().any(|item| item == "zorder:A,B"))
    }));
    assert!(records.iter().any(|record| {
        record["kind"] == "pqbench.experiment-trial" && record["name"] == "control"
    }));
    assert!(records.iter().any(|record| {
        record["kind"] == "pqbench.experiment-trial" && record["name"] == "sort:text"
    }));
    assert!(records
        .iter()
        .any(|record| record["kind"] == "pqbench.experiment-column"
            && record["column"].as_str().is_some()));
    let end = records.last().unwrap();
    assert_eq!(end["event"], "end");
    assert_eq!(end["kind"], "pqbench.experiment");
    assert!(end["trial_count"].as_u64().unwrap() >= 2);
}

#[test]
fn experiment_rejects_a_bad_aim() {
    let output = pqbench()
        .args([
            "experiment",
            parquet_fixture(),
            "--rows",
            "first:16",
            "--aim",
            "compression",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("aim"), "{stderr}");
}

#[test]
fn experiment_rejects_an_unknown_rewrite() {
    let output = pqbench()
        .args([
            "experiment",
            parquet_fixture(),
            "--rows",
            "first:16",
            "--rewrite",
            "correlate:text",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("rewrite"), "{stderr}");
}

#[test]
fn experiment_supports_zorder_cast_and_encoding() {
    let output = pqbench()
        .args([
            "experiment",
            parquet_fixture(),
            "--rows",
            "first:256",
            "--rewrite",
            "zorder:communityName,dataType",
            "--rewrite",
            "cast:dataType:string",
            "--rewrite",
            "encoding:delta_byte_array",
            "--aim",
            "all",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    for name in [
        "zorder:communityName,dataType",
        "cast:dataType:string",
        "encoding:delta_byte_array",
    ] {
        assert!(
            records.iter().any(|record| {
                record["kind"] == "pqbench.experiment-trial" && record["name"] == name
            }),
            "missing trial {name}"
        );
    }
}
