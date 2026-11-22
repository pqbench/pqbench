//! The web app: the master's status page and its records, read-only over
//! HTTP.
//!
//! `GET /` is the page; `GET /v1/{name}` is records, as the page reads them:
//!
//! - `/v1/{parent}/partitions`, parent segments `-` for any (AIP-159): every
//!   matching version, its row with its listing and price; the listing's file
//!   references keep only their size and time (a partial response, as with
//!   an AIP-157 field mask), since a warehouse has many versions;
//! - `/v1/{partition}`: that partition's versions with every result, the
//!   column masses and suggestions included.
//!
//! Nothing is computed here beyond reading the store: rollups and costs are
//! the page's sums over these records.

use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::master::{Entry, Master};

const PAGE: &str = include_str!("web/index.html");

/// The largest request head read; a GET needs a few hundred bytes.
const HEAD_LIMIT: usize = 16 * 1024;

/// Listen on `address`; a taken port fails the daemon's start.
pub(crate) async fn bind(address: &str) -> Result<TcpListener, String> {
    TcpListener::bind(address)
        .await
        .map_err(|error| format!("cannot listen on {address}: {error}"))
}

/// Serve the web app until the process ends.
pub(crate) async fn run(listener: TcpListener, master: Arc<Mutex<Master>>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let master = Arc::clone(&master);
        tokio::spawn(async move {
            let _ = handle(stream, master).await;
        });
    }
}

async fn handle(mut stream: TcpStream, master: Arc<Mutex<Master>>) -> std::io::Result<()> {
    let mut head = Vec::new();
    let mut buffer = [0u8; 2048];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") && head.len() < HEAD_LIMIT {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        head.extend_from_slice(&buffer[..read]);
    }
    let head = String::from_utf8_lossy(&head);
    let mut parts = head.lines().next().unwrap_or_default().split_whitespace();
    let (method, target) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or("/"),
    );
    let path = target.split('?').next().unwrap_or("/");
    let (status, kind, body) = match (method, path) {
        ("GET", "/") => ("200 OK", "text/html; charset=utf-8", PAGE.to_string()),
        ("GET", path) if path.starts_with("/v1/") => {
            let name = path.trim_start_matches("/v1/").trim_end_matches('/');
            let records = tokio::task::spawn_blocking({
                let name = name.to_string();
                move || records(&master, &name)
            })
            .await
            .unwrap_or_default();
            match records {
                Some(records) => (
                    "200 OK",
                    "application/json",
                    Value::Array(records).to_string(),
                ),
                None => ("404 Not Found", "application/json", "[]".to_string()),
            }
        }
        ("GET", _) => ("404 Not Found", "text/plain", "not found\n".to_string()),
        _ => (
            "405 Method Not Allowed",
            "text/plain",
            "only GET\n".to_string(),
        ),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await
}

/// The records `name` names: a partitions collection (parents may be `-`) or
/// one partition. `None` when it names neither.
fn records(master: &Mutex<Master>, name: &str) -> Option<Vec<Value>> {
    let segments: Vec<&str> = name.split('/').collect();
    let collection = segments.len() == 7 && segments[6] == "partitions";
    let one = segments.len() == 8 && segments[6] == "partitions" && !segments.contains(&"-");
    if !collection && !one {
        return None;
    }
    let pattern = |row: &str| {
        let row: Vec<&str> = row.split('/').collect();
        row.len() == 8
            && segments
                .iter()
                .zip(&row)
                .all(|(want, have)| *want == "-" || want == have)
    };
    let (store, rows): (std::path::PathBuf, Vec<Entry>) = {
        let master = master
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let rows = master
            .list(None)
            .into_iter()
            .filter(|entry| pattern(&entry.name))
            .collect();
        (master.store().to_path_buf(), rows)
    };
    let outputs: &[&str] = match collection {
        true => &["list-partitions", "price"],
        false => &["list-partitions", "price", "bytemass", "list-suggestions"],
    };
    Some(
        rows.iter()
            .map(|entry| {
                let mut view = view(&store, entry, outputs);
                if collection {
                    trim_files(&mut view);
                }
                view
            })
            .collect(),
    )
}

/// Keep only the size and time of the listing's file references.
fn trim_files(view: &mut Value) {
    let Some(listing) = view
        .get_mut("list_partitions")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for field in ["first_file", "last_file", "sample_file"] {
        if let Some(Value::Object(file)) = listing.get_mut(field) {
            file.retain(|key, _| key == "size_bytes" || key == "modification_time");
        }
    }
}

/// One version as the page reads it: the row, then each stored result under
/// its command's name (a one-line result as a record, longer ones as arrays).
fn view(store: &Path, entry: &Entry, outputs: &[&str]) -> Value {
    let mut view = match serde_json::to_value(entry) {
        Ok(Value::Object(map)) => map,
        _ => Map::new(),
    };
    let directory = store.join("results").join(&entry.name).join(&entry.etag);
    for output in outputs {
        let Ok(text) = std::fs::read_to_string(directory.join(format!("{output}.ndjson"))) else {
            continue;
        };
        let lines: Vec<Value> = text
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        let value = match *output {
            "list-partitions" | "price" => lines.into_iter().next().unwrap_or(Value::Null),
            _ => Value::Array(lines),
        };
        view.insert(output.replace('-', "_"), value);
    }
    Value::Object(view)
}
