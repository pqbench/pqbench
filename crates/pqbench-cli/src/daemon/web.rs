//! The web app: the master's partition versions, read-only over HTTP.
//!
//! `GET /` is the page; `GET /v1/{name}` reads records:
//!
//! - `/v1/{parent}/partitions`, parent segments `-` for any (AIP-159): one
//!   page of versions (AIP-158 `page_size`, `page_token`; AIP-132 `order_by`
//!   on a row field, ` desc` to reverse), each its row with its listing and
//!   price; the listing's file references keep only their size and time;
//! - `/v1/{partition}`: that partition's versions with every result, the
//!   column masses and suggestions included.
//!
//! The order and the page come from the master's rows in memory; only the
//! page's versions read their results from the store.

use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::master::{entry, Entry, Key, Master, Row};

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

/// What `name` names: a page of a partitions collection (parents may be
/// `-`), or one partition's versions. `None` when it names neither.
fn read(master: &Mutex<Master>, name: &str, query: &str) -> Result<Option<Value>, String> {
    let segments: Vec<&str> = name.split('/').collect();
    let collection = segments.len() == 7 && segments[6] == "partitions";
    let one = segments.len() == 8 && segments[6] == "partitions" && !segments.contains(&"-");
    if !collection && !one {
        return Ok(None);
    }
    let matches = |row: &str| {
        let row: Vec<&str> = row.split('/').collect();
        row.len() == 8
            && segments
                .iter()
                .zip(&row)
                .all(|(want, have)| *want == "-" || want == have)
    };
    if one {
        let (store, rows) = {
            let master = lock(master);
            let rows: Vec<Entry> = master
                .rows()
                .filter(|((row, _), _)| row == name)
                .map(|((name, etag), row)| entry(name, etag, row))
                .collect();
            (master.store().to_path_buf(), rows)
        };
        let outputs = ["list-partitions", "price", "bytemass", "list-suggestions"];
        let versions = rows.iter().map(|row| view(&store, row, &outputs));
        return Ok(Some(Value::Array(versions.collect())));
    }
    let page = Page::parse(query)?;
    let (store, total_size, rows) = {
        let master = lock(master);
        let mut rows: Vec<(&Key, &Row)> =
            master.rows().filter(|((row, _), _)| matches(row)).collect();
        if let Some(order) = page.order {
            rows.sort_by(|a, b| {
                let ordering = order.compare(a, b);
                if page.descending {
                    ordering.reverse()
                } else {
                    ordering
                }
            });
        } else if page.descending {
            rows.reverse();
        }
        let total_size = rows.len();
        let rows: Vec<Entry> = rows
            .into_iter()
            .skip(page.offset)
            .take(page.size)
            .map(|((name, etag), row)| entry(name, etag, row))
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

/// A list request's page: where it starts, how many, in which order.
struct Page {
    offset: usize,
    size: usize,
    order: Option<Order>,
    descending: bool,
}

/// A row field a list can be ordered by; `None` is the key order (`name`).
#[derive(Clone, Copy)]
enum Order {
    SizeBytes,
    FileCount,
    RemovedSizeBytes,
    CreateTime,
    FailureCount,
}

impl Order {
    /// Compare two rows by the field, then by key.
    fn compare(self, (a_key, a): &(&Key, &Row), (b_key, b): &(&Key, &Row)) -> std::cmp::Ordering {
        let field = |row: &Row| match self {
            Order::SizeBytes => row.size_bytes,
            Order::FileCount => row.file_count,
            Order::RemovedSizeBytes => row.removed_size_bytes.unwrap_or(0),
            Order::CreateTime => row.create_time,
            Order::FailureCount => row.failure_count,
        };
        field(a).cmp(&field(b)).then_with(|| a_key.cmp(b_key))
    }
}

impl Page {
    fn parse(query: &str) -> Result<Self, String> {
        let mut page = Page {
            offset: 0,
            size: DEFAULT_PAGE_SIZE,
            order: None,
            descending: false,
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
                    let mut words = value.split_whitespace();
                    let field = words.next().unwrap_or("name");
                    page.descending = match words.next() {
                        None | Some("asc") => false,
                        Some("desc") => true,
                        Some(other) => return Err(format!("order_by direction {other:?}")),
                    };
                    page.order = match field {
                        "name" => None,
                        "size_bytes" => Some(Order::SizeBytes),
                        "file_count" => Some(Order::FileCount),
                        "removed_size_bytes" => Some(Order::RemovedSizeBytes),
                        "create_time" => Some(Order::CreateTime),
                        "failure_count" => Some(Order::FailureCount),
                        other => return Err(format!("cannot order by {other:?}")),
                    };
                }
                _ => {}
            }
        }
        Ok(page)
    }
}

fn lock(master: &Mutex<Master>) -> std::sync::MutexGuard<'_, Master> {
    master
        .lock()
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
