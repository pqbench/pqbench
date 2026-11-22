#![cfg(feature = "aws")]

use std::io::{Read, Write};
use std::process::{Command, Stdio};

use serde_json::{json, Value};

const GIBIBYTE: f64 = 1_073_741_824.0;

/// An S3 stand-in that answers every HEAD for a key containing a needle with
/// that key's storage class (none for STANDARD, as S3 does), and counts them.
fn head_server(
    objects: &'static [(&'static str, Option<&'static str>)],
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let heads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = heads.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().take(32) {
            let mut stream = stream.unwrap();
            let mut buffer = [0u8; 4096];
            let read = stream.read(&mut buffer).unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]);
            let line = request.lines().next().unwrap_or_default();
            let response = match objects.iter().find(|(needle, _)| line.contains(needle)) {
                Some((_, class)) if line.starts_with("HEAD ") => {
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let class = class
                        .map(|class| format!("x-amz-storage-class: {class}\r\n"))
                        .unwrap_or_default();
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nETag: \"e\"\r\n{class}Connection: close\r\n\r\n"
                    )
                }
                _ => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_string(),
            };
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (address, heads)
}

fn get_partition_price(stdin: &str, endpoint: &str) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .args(["get", "partition-price"])
        .env("AWS_ENDPOINT_URL", endpoint)
        .env("AWS_ENDPOINT", endpoint)
        .env("AWS_ALLOW_HTTP", "true")
        .env("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_REGION", "us-east-1")
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

fn file(blob: &str) -> Value {
    json!({"path": "x.parquet", "blob": blob})
}

/// A partition of `size_bytes`, with `removed` bytes, whose oldest, sample
/// and newest files are the given blobs.
fn partition(id: &str, blobs: [&str; 3], size_bytes: u64, removed: u64) -> String {
    format!(
        "{}\n",
        json!({
            "name": format!("catalogs/c/schemas/s/tables/t/partitions/{id}"),
            "etag": "00000000000000ab",
            "file_count": 3,
            "size_bytes": size_bytes,
            "smallest_file_size_bytes": 1,
            "largest_file_size_bytes": 1,
            "first_file": file(blobs[0]),
            "sample_file": file(blobs[1]),
            "last_file": file(blobs[2]),
            "removed_file_count": 1,
            "removed_size_bytes": removed,
        })
    )
}

fn records(output: &std::process::Output) -> Vec<Value> {
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("ndjson line"))
        .collect()
}

/// One class on every file prices the partition and its removed bytes at it;
/// a file named twice is HEADed once.
#[test]
fn get_partition_price_prices_the_bytes_at_the_files_class() {
    let (endpoint, heads) =
        head_server(&[("/lake/old.parquet", None), ("/lake/new.parquet", None)]);
    let stdin = partition(
        "day=1",
        [
            "blobs/s3/lake%2Fold.parquet",
            "blobs/s3/lake%2Fnew.parquet",
            "blobs/s3/lake%2Fnew.parquet",
        ],
        10 * GIBIBYTE as u64,
        2 * GIBIBYTE as u64,
    );
    let records = records(&get_partition_price(&stdin, &endpoint));
    assert_eq!(records.len(), 1);
    let price = &records[0];
    assert_eq!(
        price["name"],
        "catalogs/c/schemas/s/tables/t/partitions/day%3D1/price"
    );
    assert_eq!(price["partition_etag"], "00000000000000ab");
    assert_eq!(price["storage_classes"], json!(["STANDARD"]));
    let cost = price["storage_costs"][0]["cost_per_month"]
        .as_f64()
        .unwrap();
    assert!((cost - 10.0 * 0.023).abs() < 1e-9, "{price}");
    let removed = price["removed_storage_costs"][0]["cost_per_month"]
        .as_f64()
        .unwrap();
    assert!((removed - 2.0 * 0.023).abs() < 1e-9, "{price}");
    assert_eq!(heads.load(std::sync::atomic::Ordering::SeqCst), 2);
}

/// An older file in a colder class than a newer one is a lifecycle transition
/// inside the partition: both classes, oldest first, each pricing the bytes.
#[test]
fn get_partition_price_names_a_transition_inside_the_partition() {
    let (endpoint, _) = head_server(&[
        ("/lake/old.parquet", Some("GLACIER_IR")),
        ("/lake/new.parquet", None),
    ]);
    let stdin = partition(
        "day=1",
        [
            "blobs/s3/lake%2Fold.parquet",
            "blobs/s3/lake%2Fnew.parquet",
            "blobs/s3/lake%2Fnew.parquet",
        ],
        GIBIBYTE as u64,
        0,
    );
    let price = &records(&get_partition_price(&stdin, &endpoint))[0];
    assert_eq!(price["storage_classes"], json!(["GLACIER_IR", "STANDARD"]));
    let costs: Vec<f64> = price["storage_costs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|cost| cost["cost_per_month"].as_f64().unwrap())
        .collect();
    assert_eq!(costs.len(), 2);
    assert!((costs[0] - 0.004).abs() < 1e-9 && (costs[1] - 0.023).abs() < 1e-9);
    assert!(price.get("removed_storage_costs").is_none(), "{price}");
}

/// Local files have no storage class: no HEAD, no class, no cost, and the
/// record still says which etag it priced.
#[test]
fn get_partition_price_leaves_local_files_unpriced() {
    let (endpoint, heads) = head_server(&[]);
    let stdin = partition(
        "day=1",
        [
            "blobs/file/%2Ftmp%2Fa.parquet",
            "blobs/file/%2Ftmp%2Fa.parquet",
            "blobs/file/%2Ftmp%2Fa.parquet",
        ],
        100,
        0,
    );
    let price = &records(&get_partition_price(&stdin, &endpoint))[0];
    assert_eq!(price["partition_etag"], "00000000000000ab");
    assert!(price.get("storage_classes").is_none(), "{price}");
    assert!(price.get("storage_costs").is_none(), "{price}");
    assert_eq!(heads.load(std::sync::atomic::Ordering::SeqCst), 0);
}

/// A HEAD that fails is reported under the partition; the others still price.
#[test]
fn get_partition_price_keeps_the_partitions_that_price() {
    let (endpoint, _) = head_server(&[("/lake/new.parquet", None)]);
    let stdin = format!(
        "{}{}",
        partition("day=1", ["blobs/s3/lake%2Fgone.parquet"; 3], 1, 0),
        partition("day=2", ["blobs/s3/lake%2Fnew.parquet"; 3], 1, 0)
    );
    let output = get_partition_price(&stdin, &endpoint);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("partitions/day%3D1"), "stderr: {stderr}");
    let prices: Vec<Value> = output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(prices.len(), 1);
    assert!(prices[0]["name"].as_str().unwrap().contains("day%3D2"));
}
