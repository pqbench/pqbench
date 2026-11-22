//! The web app: the master's partition versions, read-only over HTTP.
//!
//! `GET /` is the page; `GET /v1/{name}` reads records:
//!
//! - `/v1/catalogs`, `/v1/catalogs/{c}/schemas`,
//!   `/v1/catalogs/{c}/schemas/{s}/tables`, ids `-` for any (AIP-159): the
//!   master's PartitionStats, dearest first and then by size;
//! - `/v1/{table}/partitions`: one page of the table's versions (AIP-158
//!   `page_size`, `page_token`; AIP-132 `order_by`, dearest first by default
//!   or `last_modification_time desc`), each its row with its listing and
//!   price; the listing's file references keep only their size and time;
//! - `/v1/{partition}`: that partition's versions with every result, the
//!   column masses and suggestions included.
//!
//! A level's page reads the master's stats file; a partitions page sorts the
//! master's rows for the one table. Only a page's versions read their results
//! from the store.

use std::path::Path;
use std::sync::{Arc, RwLock};

use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use pqbench::v1::{PartitionStats, PartitionVersion};

use super::master::{version, Key, Master};

const PAGE: &str = include_str!("web/index.html");

/// The largest request head read; a GET needs a few hundred bytes.
const HEAD_LIMIT: usize = 16 * 1024;

/// A list's page size when the request names none, and the largest it may
/// name.
const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 1000;

/// Listen on `address`; a taken port fails the daemon's start.
pub(crate) async fn bind(address: &str) -> Result<TcpListener, String> {
    TcpListener::bind(address)
        .await
        .map_err(|error| format!("cannot listen on {address}: {error}"))
}

/// Serve the web app until the process ends.
pub(crate) async fn run(listener: TcpListener, master: Arc<RwLock<Master>>) {
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

async fn handle(mut stream: TcpStream, master: Arc<RwLock<Master>>) -> std::io::Result<()> {
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
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let (status, kind, body) = match (method, path) {
        ("GET", "/") => ("200 OK", "text/html; charset=utf-8", PAGE.to_string()),
        ("GET", path) if path.starts_with("/v1/") => {
            let name = path.trim_start_matches("/v1/").trim_end_matches('/');
            let read = tokio::task::spawn_blocking({
                let (name, query) = (name.to_string(), query.to_string());
                move || read(&master, &name, &query)
            })
            .await
            .unwrap_or_else(|error| Err(error.to_string()));
            match read {
                Ok(Some(value)) => ("200 OK", "application/json", value.to_string()),
                Ok(None) => ("404 Not Found", "application/json", "{}".to_string()),
                Err(error) => (
                    "400 Bad Request",
                    "application/json",
                    json!({ "error": error }).to_string(),
                ),
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

/// What `name` names: a level's stats, a page of one table's versions, or
/// one partition's versions. `None` when it names none of them.
fn read(master: &RwLock<Master>, name: &str, query: &str) -> Result<Option<Value>, String> {
    let segments: Vec<&str> = name.split('/').collect();
    if level(&segments) {
        return Ok(Some(Value::Array(stats(master, &segments)?)));
    }
    let collection = segments.len() == 7 && segments[6] == "partitions";
    let one = segments.len() == 8 && segments[6] == "partitions" && !segments.contains(&"-");
    if !collection && !one {
        return Ok(None);
    }
    if one {
        let (store, rows) = {
            let master = lock(master);
            let rows: Vec<PartitionVersion> = master.records(name)?;
            (master.store().to_path_buf(), rows)
        };
        let outputs = ["list-partitions", "price", "bytemass", "list-suggestions"];
        let versions = rows.iter().map(|row| view(&store, row, &outputs));
        return Ok(Some(Value::Array(versions.collect())));
    }
    let page = Page::parse(query)?;
    let parent = segments[..6].join("/");
    let (store, total_size, rows) = {
        let master = lock(master);
        let mut rows = master.list(&parent)?;
        rows.sort_by(|a, b| page.order.compare(a, b));
        let total_size = rows.len();
        let rows: Vec<PartitionVersion> = rows
            .into_iter()
            .skip(page.offset)
            .take(page.size)
            .map(|(key, row)| version(key, row))
            .collect();
        (master.store().to_path_buf(), total_size, rows)
    };
    let partitions: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut view = view(&store, row, &["list-partitions", "price"]);
            trim_files(&mut view);
            view
        })
        .collect();
    let next = page.offset + partitions.len();
    let mut response = json!({ "partitions": partitions, "total_size": total_size });
    if next < total_size {
        response["next_page_token"] = Value::String(next.to_string());
    }
    Ok(Some(response))
}

/// Whether `segments` names a level: `catalogs`, `catalogs/{c}/schemas`, or
/// `catalogs/{c}/schemas/{s}/tables`.
fn level(segments: &[&str]) -> bool {
    matches!(
        segments,
        ["catalogs"] | ["catalogs", _, "schemas"] | ["catalogs", _, "schemas", _, "tables"]
    )
}

/// The stats the stats job wrote, the ones under the `segments` (which may
/// hold `-`), dearest first and then by size: a level's page reads no
/// versions.
fn stats(master: &RwLock<Master>, segments: &[&str]) -> Result<Vec<Value>, String> {
    let file = lock(master).stats_file();
    let Some(file) = file else {
        return Ok(Vec::new());
    };
    let text = std::fs::read_to_string(&file)
        .map_err(|error| format!("cannot read the stats: {error}"))?;
    let mut rows: Vec<PartitionStats> = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let stats: PartitionStats =
            serde_json::from_str(line).map_err(|error| format!("bad stats line: {error}"))?;
        if same_parent(segments, &stats.name) {
            rows.push(stats);
        }
    }
    rows.sort_by(|a, b| {
        cost(b)
            .total_cmp(&cost(a))
            .then_with(|| b.size_bytes.cmp(&a.size_bytes))
    });
    Ok(rows
        .iter()
        .filter_map(|stats| serde_json::to_value(stats).ok())
        .collect())
}

/// Whether a stats record.s parent is what `segments` names: one segment
/// deeper, with `-` matching any id.
fn same_parent(segments: &[&str], name: &str) -> bool {
    let segments_of_name: Vec<&str> = name.split('/').collect();
    segments_of_name.len() == segments.len() + 2
        && segments
            .iter()
            .zip(&segments_of_name)
            .all(|(want, have)| *want == "-" || want == have)
}

/// A stats record's dearest monthly cost, its cheapest when it has no range.
fn cost(stats: &PartitionStats) -> f64 {
    stats
        .highest_cost_per_month
        .or(stats.lowest_cost_per_month)
        .unwrap_or(0.0)
}

/// A partitions request's page: where it starts, how many, in which order.
struct Page {
    offset: usize,
    size: usize,
    order: Order,
}

/// The two orders a table's partitions come in: dearest first, or the newest
/// file first.
#[derive(Clone, Copy)]
enum Order {
    Cost,
    NewestFile,
}

impl Order {
    /// Compare two versions by the order, then by key.
    fn compare(
        self,
        (a_key, a): &(&Key, &PartitionVersion),
        (b_key, b): &(&Key, &PartitionVersion),
    ) -> std::cmp::Ordering {
        let ordering = match self {
            Order::Cost => cost_version(b)
                .total_cmp(&cost_version(a))
                .then_with(|| b.size_bytes.cmp(&a.size_bytes)),
            Order::NewestFile => b
                .last_modification_time
                .cmp(&a.last_modification_time)
                .then_with(|| cost_version(b).total_cmp(&cost_version(a))),
        };
        ordering.then_with(|| a_key.cmp(b_key))
    }
}

fn cost_version(version: &PartitionVersion) -> f64 {
    version
        .highest_cost_per_month
        .or(version.lowest_cost_per_month)
        .unwrap_or(0.0)
}

impl Page {
    fn parse(query: &str) -> Result<Self, String> {
        let mut page = Page {
            offset: 0,
            size: DEFAULT_PAGE_SIZE,
            order: Order::Cost,
        };
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match &*key {
                "page_size" => {
                    page.size = match value.parse::<usize>() {
                        Ok(0) => DEFAULT_PAGE_SIZE,
                        Ok(size) => size.min(MAX_PAGE_SIZE),
                        Err(_) => return Err(format!("page_size {value:?} is not a count")),
                    }
                }
                "page_token" if !value.is_empty() => {
                    page.offset = value
                        .parse()
                        .map_err(|_| format!("page_token {value:?} is not one this list gave"))?;
                }
                "order_by" => {
                    page.order = match value.trim() {
                        "highest_cost_per_month desc" | "" => Order::Cost,
                        "last_modification_time desc" => Order::NewestFile,
                        other => {
                            return Err(format!(
                                "order_by {other:?}: a table's partitions come as `highest_cost_per_month desc` or `last_modification_time desc`"
                            ))
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(page)
    }
}

fn lock(master: &RwLock<Master>) -> std::sync::RwLockReadGuard<'_, Master> {
    master
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
fn view(store: &Path, version: &PartitionVersion, outputs: &[&str]) -> Value {
    let mut view = match serde_json::to_value(version) {
        Ok(Value::Object(map)) => map,
        _ => Map::new(),
    };
    let directory = store
        .join("results")
        .join(version.name.replace("/versions/", "/"));
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
