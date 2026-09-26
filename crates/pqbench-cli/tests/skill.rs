use std::process::Command;

fn pqbench() -> Command {
    Command::new(env!("CARGO_BIN_EXE_pqbench"))
}

fn ndjson_records(stdout: &[u8]) -> Vec<serde_json::Value> {
    stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

#[test]
fn skill_lists_the_advisor() {
    let output = pqbench().args(["skill"]).output().unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson_records(&output.stdout);
    assert!(records.iter().any(|record| {
        record["kind"] == "pqbench.skill" && record["name"] == "parquet-advisor"
    }));
}

#[test]
fn skill_prints_recipes_with_levels_and_ddl() {
    let output = pqbench()
        .args(["skill", "parquet-advisor"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let body = String::from_utf8_lossy(&output.stdout);
    assert!(body.contains("pqbench experiment"), "{body}");
    assert!(body.contains("zstd@3"), "{body}");
    let recipes = pqbench()
        .args(["skill", "parquet-advisor", "recipes"])
        .output()
        .unwrap();
    assert!(recipes.status.success());
    let text = String::from_utf8_lossy(&recipes.stdout);
    assert!(text.contains("WRITE ORDERED BY"), "{text}");
    assert!(text.contains("zstd@1"), "{text}");
    assert!(text.contains("Pros"), "{text}");
}

#[test]
fn unknown_skill_fails() {
    let output = pqbench().args(["skill", "oracle"]).output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("parquet-advisor"), "{stderr}");
}
