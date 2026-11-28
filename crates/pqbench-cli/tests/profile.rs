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
fn profile_streams_column_facts_for_a_sample() {
    let output = pqbench()
        .args(["profile", parquet_fixture()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let columns: Vec<&serde_json::Value> = records
        .iter()
        .filter(|record| {
            record["name"]
                .as_str()
                .is_some_and(|name| name.ends_with("/profile"))
        })
        .collect();
    assert!(!columns.is_empty(), "{records:?}");
    let column = columns[0];
    assert!(column["id"].is_string());
    assert!(column["column"].is_string());
    assert!(column["null_fraction"].is_number());
    assert!(column["ndv"].is_number());
}

#[test]
fn profile_honours_columns_and_rows() {
    let output = pqbench()
        .args([
            "profile",
            parquet_fixture(),
            "--rows",
            "first:32",
            "--columns",
            "text",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    let columns: Vec<&serde_json::Value> = records
        .iter()
        .filter(|record| {
            record["name"]
                .as_str()
                .is_some_and(|name| name.ends_with("/profile"))
        })
        .collect();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["column"], "text");
    assert_eq!(columns[0]["num_values"], 32);
}

#[test]
fn profile_rejects_a_bad_rows_method() {
    let output = pqbench()
        .args(["profile", parquet_fixture(), "--rows", "last:5"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--rows"), "{stderr}");
}

#[test]
fn profile_preserves_physical_types_exact_i64_order_and_v1_round_trip() {
    use pqbench::third_party::parquet::api::{
        write_parquet, Kind, TypedColumn, Value, WriteOptions,
    };

    let dir = std::env::temp_dir().join(format!("pqbench-profile-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("types.parquet");

    let columns = [
        TypedColumn {
            name: "zip".into(),
            kind: Kind::Bytes,
        },
        TypedColumn {
            name: "big_id".into(),
            kind: Kind::Integer,
        },
    ];
    let rows = vec![
        vec![
            Value::Bytes(b"00123".to_vec()),
            Value::Integer(9_007_199_254_740_993),
        ],
        vec![
            Value::Bytes(b"00042".to_vec()),
            Value::Integer(9_007_199_254_740_992),
        ],
    ];
    let options = WriteOptions {
        compression: "uncompressed".into(),
        level: None,
        dictionary: false,
        dictionary_bytes: None,
        encoding: None,
        page_size: None,
        stats_truncate_length: None,
        sorting_columns: Vec::new(),
        row_group_size: 0,
    };
    let bytes = write_parquet(&columns, &rows, &options).unwrap();
    std::fs::write(&path, bytes).unwrap();

    let output = pqbench()
        .args(["profile", path.to_str().unwrap(), "--top", "0"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = ndjson(&output.stdout);
    assert_eq!(records.len(), 2, "{records:?}");
    let profiles: Vec<pqbench::v1::ColumnProfile> = records
        .iter()
        .map(|record| serde_json::from_value(record.clone()).expect("v1::ColumnProfile round-trip"))
        .collect();
    assert_eq!(profiles[0].column, "zip");
    assert_eq!(profiles[0].physical_kind, "string");
    assert!(profiles[0].top_values.is_empty());
    assert_eq!(profiles[1].column, "big_id");
    assert_eq!(profiles[1].physical_kind, "integer");
    assert_eq!(profiles[1].min_value.as_deref(), Some("9007199254740992"));
    assert_eq!(profiles[1].max_value.as_deref(), Some("9007199254740993"));
    assert_eq!(profiles[1].monotonic, "DECREASING");

    let _ = std::fs::remove_dir_all(&dir);
}
