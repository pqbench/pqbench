//! The web app: the master's partition versions, read-only over HTTP.
//!
//! The page is the same tables `pqbench format` prints: one ASCII table per
//! resource, and every row links to the resource it names. `GET /` is the
//! catalogs, `/catalogs/{c}` their schemas, `…/tables/{t}` the table's
//! partitions, `…/partitions/{p}` that partition's versions with its columns
//! and files. No script runs in the browser.
//!
//! `GET /v1/{name}` reads the same records as JSON:
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
//! `/v1/partitions` is the master's map whole: every version, the newest
//! found first, one page at a time. It reads no stats file, so it shows what
//! the master has as soon as it has it.
//!
//! A level's page reads the master's stats file; a partitions page sorts the
//! master's rows for the one table. Only a page's versions read their results
//! from the store.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use pqbench::v1::partition_version::State;
use pqbench::v1::{PartitionStats, PartitionVersion};

use crate::render::{render_cells, Align};

use super::master::{version, Key, Master};

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
        ("GET", path) if !path.starts_with("/v1/") => {
            let page = tokio::task::spawn_blocking({
                let (path, master) = (path.to_string(), Arc::clone(&master));
                move || page(&master, &path)
            })
            .await
            .unwrap_or_else(|error| Err(error.to_string()));
            match page {
                Ok(body) => ("200 OK", "text/html; charset=utf-8", body),
                Err(error) => (
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    format!("{error}\n"),
                ),
            }
        }
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

/// A page: the ASCII table of one resource, every row a link to what it
/// names. `GET /v1/` is the JSON; this is the same data as `pqbench format`
/// prints.
fn page(master: &RwLock<Master>, path: &str) -> Result<String, String> {
    let name = path.trim_matches('/');
    let segments: Vec<&str> = if name.is_empty() {
        Vec::new()
    } else {
        name.split('/').collect()
    };
    // A level's records are the ones under its collection name: `/` is the
    // catalogs, `/catalogs/{c}` their schemas, `…/schemas/{s}` their tables.
    let body = match segments.len() {
        0 => levels_page(master, &["catalogs"])?,
        1 if segments[0] == "partitions" => recent_page(master)?,
        1 if segments[0] == "insights" => cached_insights(master)?,
        2 => levels_page(master, &[segments[0], segments[1], "schemas"])?,
        3 if segments[2] == "schemas" => levels_page(master, &segments)?,
        4 => levels_page(
            master,
            &[segments[0], segments[1], segments[2], segments[3], "tables"],
        )?,
        5 if segments[4] == "tables" => levels_page(master, &segments)?,
        6 => partitions_page(master, name)?,
        8 => partition_page(master, name)?,
        _ => return Err(format!("no page for /{name}")),
    };
    Ok(shell(&crumbs(&segments), &body))
}

/// The catalogs, schemas or tables under a collection, dearest first.
fn levels_page(master: &RwLock<Master>, segments: &[&str]) -> Result<String, String> {
    let rows = stats(master, segments)?;
    let cells: Vec<(String, Vec<String>)> = rows
        .iter()
        .map(|row| {
            let name = row.name.trim_end_matches("/partitionStats").to_string();
            (
                name.clone(),
                vec![
                    name,
                    row.partition_count.to_string(),
                    row.file_count.to_string(),
                    size_text(row.size_bytes),
                    money(row.highest_cost_per_month.or(row.lowest_cost_per_month)),
                    row.version_counts
                        .iter()
                        .map(|(state, count)| format!("{} {count}", state_name(*state)))
                        .collect::<Vec<_>>()
                        .join(", "),
                    row.failed_version_count.to_string(),
                    when(row.last_modification_time),
                ],
            )
        })
        .collect();
    Ok(ascii(
        &[
            "name",
            "partitions",
            "files",
            "bytes",
            "stock /mo",
            "versions",
            "failed",
            "newest file",
        ],
        cells,
    ))
}

/// One table's partitions, dearest first.
fn partitions_page(master: &RwLock<Master>, table: &str) -> Result<String, String> {
    let (rows, listed): (Vec<PartitionVersion>, usize) = {
        let master = lock(master);
        let mut rows = master.list(table)?;
        rows.sort_by(|a, b| cost_version(b.1).total_cmp(&cost_version(a.1)));
        let listed = rows.len();
        let rows: Vec<PartitionVersion> = rows
            .into_iter()
            .take(MAX_ROWS)
            .map(|(key, row)| version(key, row))
            .collect();
        (rows, listed)
    };
    let cells: Vec<(String, Vec<String>)> = rows
        .iter()
        .map(|row| {
            (
                partition_name(&row.name),
                vec![
                    row.name.clone(),
                    state_name(row.state).to_string(),
                    row.file_count.to_string(),
                    size_text(row.size_bytes),
                    size_text(row.removed_size_bytes.unwrap_or(0)),
                    money(Some(cost_version(row))),
                    when(Some(row.create_time)),
                    row.failure_count.to_string(),
                ],
            )
        })
        .collect();
    let table = ascii(
        &[
            "partition",
            "state",
            "files",
            "bytes",
            "removed",
            "stock /mo",
            "found",
            "failures",
        ],
        cells,
    );
    Ok(note_if_truncated(table, rows.len(), listed))
}

/// The map whole: every version, the newest found first.
fn recent_page(master: &RwLock<Master>) -> Result<String, String> {
    let (rows, listed) = {
        let master = lock(master);
        let mut rows = master.list("")?;
        rows.sort_by(|(a_key, a), (b_key, b)| {
            b.create_time
                .cmp(&a.create_time)
                .then_with(|| a_key.cmp(b_key))
        });
        let listed = rows.len();
        let rows: Vec<PartitionVersion> = rows
            .into_iter()
            .take(MAX_ROWS)
            .map(|(key, row)| version(key, row))
            .collect();
        (rows, listed)
    };
    let cells: Vec<(String, Vec<String>)> = rows
        .iter()
        .map(|row| {
            let partition = row.name.split("/versions/").next().unwrap_or_default();
            (
                partition.to_string(),
                vec![
                    row.name.clone(),
                    state_name(row.state).to_string(),
                    row.file_count.to_string(),
                    size_text(row.size_bytes),
                    money(Some(cost_version(row))),
                    when(Some(row.create_time)),
                ],
            )
        })
        .collect();
    let table = ascii(
        &["version", "state", "files", "bytes", "stock /mo", "found"],
        cells,
    );
    Ok(note_if_truncated(table, rows.len(), listed))
}

/// One partition: its versions, then the newest's columns and files.
fn partition_page(master: &RwLock<Master>, partition: &str) -> Result<String, String> {
    let (store, mut rows) = {
        let master = lock(master);
        (master.store().to_path_buf(), master.records(partition)?)
    };
    if rows.is_empty() {
        return Err(format!("no such partition: {partition}"));
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row.create_time));
    let cells: Vec<(String, Vec<String>)> = rows
        .iter()
        .map(|row| {
            (
                String::new(),
                vec![
                    row.name.rsplit('/').next().unwrap_or_default().to_string(),
                    state_name(row.state).to_string(),
                    row.file_count.to_string(),
                    size_text(row.size_bytes),
                    money(Some(cost_version(row))),
                    when(Some(row.create_time)),
                    when(row.up_to_date_time),
                    row.failure_count.to_string(),
                ],
            )
        })
        .collect();
    let mut body = ascii(
        &[
            "version",
            "state",
            "files",
            "bytes",
            "stock /mo",
            "found",
            "done",
            "failures",
        ],
        cells,
    );
    let current = rows
        .iter()
        .find(|row| row.state != State::Deleted)
        .unwrap_or(&rows[0]);
    let view = view(&store, current, &["list-partitions", "price", "bytemass"]);
    let masses: Vec<&Value> = view["bytemass"]
        .as_array()
        .map(|rows| rows.iter().collect())
        .unwrap_or_default();
    let columns: Vec<(String, Vec<String>)> = masses
        .iter()
        .filter(|row| row["mass"].is_object())
        .map(|row| {
            let mass = &row["mass"];
            (
                String::new(),
                vec![
                    row["name"]
                        .as_str()
                        .unwrap_or_default()
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .to_string(),
                    row["physical_data_type"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    mass["codec"].as_str().unwrap_or_default().to_string(),
                    if mass["dictionary"].as_bool().unwrap_or(false) {
                        format!(
                            "yes {}",
                            size_text(mass["dictionary_bytes"].as_u64().unwrap_or(0))
                        )
                    } else {
                        "no".to_string()
                    },
                    count(mass["value_count"].as_u64()),
                    count(mass["null_count"].as_u64()),
                    size_text(mass["compressed_bytes"].as_u64().unwrap_or(0)),
                    size_text(mass["uncompressed_bytes"].as_u64().unwrap_or(0)),
                    number(mass["compression_ratio"].as_f64()),
                    number(mass["bytes_per_row"].as_f64()),
                ],
            )
        })
        .collect();
    if !columns.is_empty() {
        body.push('\n');
        body.push_str(&ascii(
            &[
                "column",
                "type",
                "codec",
                "dictionary",
                "values",
                "nulls",
                "compressed",
                "uncompressed",
                "ratio",
                "bytes/row",
            ],
            columns,
        ));
    }
    let listing = &view["list_partitions"];
    let files: Vec<(String, Vec<String>)> = ["first_file", "last_file", "sample_file"]
        .iter()
        .filter_map(|field| listing.get(*field).filter(|file| file.is_object()))
        .map(|file| {
            (
                String::new(),
                vec![
                    file["path"].as_str().unwrap_or_default().to_string(),
                    size_text(file["size_bytes"].as_u64().unwrap_or(0)),
                    when(file["modification_time"].as_u64()),
                ],
            )
        })
        .collect();
    if !files.is_empty() {
        body.push('\n');
        body.push_str(&ascii(&["file", "size", "modified"], files));
    }
    Ok(body)
}
/// The insights page reads every live version's results, so a refresh within
/// [`INSIGHTS_TTL`] shows the render it just made.
static INSIGHTS: std::sync::Mutex<Option<(std::time::Instant, String)>> =
    std::sync::Mutex::new(None);
const INSIGHTS_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// The latest storage class each live version reports, or the one that
/// covers most of it, with the monthly cost that class carries there.
fn storage_class(costs: &[Value]) -> Option<(String, f64)> {
    costs
        .iter()
        .filter_map(|cost| {
            let class = cost["storage_class"].as_str()?;
            let tier = cost["access_tier"].as_str().unwrap_or_default();
            let name = if tier.is_empty() {
                class.to_string()
            } else {
                format!("{class} / {tier}")
            };
            Some((name, cost["cost_per_month"].as_f64().unwrap_or(0.0)))
        })
        .max_by(|a, b| a.1.total_cmp(&b.1))
}

/// The insights page, from the cache while it is fresh.
fn cached_insights(master: &RwLock<Master>) -> Result<String, String> {
    let mut cache = INSIGHTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some((at, text)) = cache.as_ref() {
        if at.elapsed() < INSIGHTS_TTL {
            return Ok(text.clone());
        }
    }
    let text = insights_page(master)?;
    *cache = Some((std::time::Instant::now(), text.clone()));
    Ok(text)
}

/// The insights: what the versions found, folded into the three questions the
/// tool exists to answer — where the bytes and the money are by storage class,
/// what was removed and not vacuumed, and what the suggestions say by rule.
fn insights_page(master: &RwLock<Master>) -> Result<String, String> {
    let (store, versions): (PathBuf, Vec<PartitionVersion>) = {
        let master = lock(master);
        let rows: Vec<PartitionVersion> = master
            .list("")?
            .into_iter()
            .filter(|(_, row)| row.state != State::Deleted)
            .map(|(key, row)| version(key, row))
            .collect();
        (master.store().to_path_buf(), rows)
    };
    let mut classes: BTreeMap<String, (u64, f64)> = BTreeMap::new();
    let mut removed: BTreeMap<String, (u64, u64, f64)> = BTreeMap::new();
    let mut rules: BTreeMap<String, Rule> = BTreeMap::new();
    for row in &versions {
        let directory = store
            .join("results")
            .join(row.name.replace("/versions/", "/"));
        let table = row
            .name
            .split_once("/partitions/")
            .map_or("", |(table, _)| table)
            .to_string();
        let price = read_lines(&directory.join("price.ndjson"));
        if let Some(price) = price.first() {
            if let Some((class, cost)) = storage_class(
                price["storage_costs"]
                    .as_array()
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            ) {
                let entry = classes.entry(class).or_default();
                entry.0 += 1;
                entry.1 += cost;
            }
            let removed_cost = price["removed_storage_costs"]
                .as_array()
                .map(|costs| {
                    costs
                        .iter()
                        .filter_map(|cost| cost["cost_per_month"].as_f64())
                        .sum()
                })
                .unwrap_or(0.0);
            let entry = removed.entry(table.clone()).or_default();
            entry.0 += 1;
            entry.1 += row.removed_size_bytes.unwrap_or(0);
            entry.2 += removed_cost;
        }
        for suggestion in read_lines(&directory.join("list-suggestions.ndjson")) {
            let Some(rule) = suggestion["rule"].as_str() else {
                continue;
            };
            let rule = rules.entry(rule.to_string()).or_default();
            rule.suggestions += 1;
            if let Some(column) = suggestion["column"].as_str() {
                rule.columns.insert(column.to_string());
            }
            rule.tables.insert(table.clone());
            if rule.message.is_none() {
                rule.message = suggestion["message"]
                    .as_str()
                    .or_else(|| suggestion["remedy"].as_str())
                    .map(str::to_string);
            }
        }
    }
    let mut body = String::new();
    let class_cells: Vec<(String, Vec<String>)> = classes
        .iter()
        .map(|(class, (count, cost))| {
            (
                String::new(),
                vec![class.clone(), count.to_string(), money(Some(*cost))],
            )
        })
        .collect();
    body.push_str(&ascii(
        &["storage class", "versions", "stock /mo"],
        class_cells,
    ));
    let removed_cells: Vec<(String, Vec<String>)> = removed
        .iter()
        .filter(|(_, (_, bytes, _))| *bytes > 0)
        .map(|(table, (partitions, bytes, cost))| {
            (
                table.clone(),
                vec![
                    table.clone(),
                    partitions.to_string(),
                    size_text(*bytes),
                    money(Some(*cost)),
                ],
            )
        })
        .collect();
    body.push('\n');
    body.push_str(&ascii(
        &["table", "partitions", "removed", "removed /mo"],
        removed_cells,
    ));
    let rule_cells: Vec<(String, Vec<String>)> = rules
        .iter()
        .map(|(rule, counts)| {
            (
                String::new(),
                vec![
                    rule.clone(),
                    counts.suggestions.to_string(),
                    counts.tables.len().to_string(),
                    counts.columns.len().to_string(),
                    counts.message.clone().unwrap_or_default(),
                ],
            )
        })
        .collect();
    body.push('\n');
    body.push_str(&ascii(
        &["rule", "suggestions", "tables", "columns", "what it says"],
        rule_cells,
    ));
    Ok(body)
}

/// One rule's suggestions across the map.
#[derive(Default)]
struct Rule {
    suggestions: u64,
    tables: BTreeSet<String>,
    columns: BTreeSet<String>,
    message: Option<String>,
}

/// The JSON lines a result file holds, or none.
fn read_lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .map(|text| {
            text.lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// The most rows one page shows; the map can be larger than a screen.
const MAX_ROWS: usize = 1000;

/// One ASCII table, every data line a link to the resource it names.
fn ascii(header: &[&str], cells: Vec<(String, Vec<String>)>) -> String {
    let align: Vec<Align> = header.iter().map(|_| Align::Left).collect();
    let rows: Vec<Vec<String>> = cells.iter().map(|(_, cells)| cells.clone()).collect();
    let text = render_cells(header, &align, &rows);
    let mut lines = text.lines();
    let mut out = String::new();
    for line in lines.by_ref().take(2) {
        out.push_str(&esc(line));
        out.push('\n');
    }
    for (line, (href, _)) in lines.zip(&cells) {
        let line = esc(line);
        if href.is_empty() {
            out.push_str(&line);
            out.push('\n');
        } else {
            out.push_str(&format!("<a href=\"/{}\">{line}</a>\n", esc(href)));
        }
    }
    out
}

/// The page shell: the crumbs, then the table.
fn shell(crumbs: &str, body: &str) -> String {
    format!(
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\
         <title>pqbench</title><style>\
         body{{margin:0;background:#faf8f5;color:#26231f;font:13px/1.45 ui-monospace,monospace}}\
         header{{padding:8px 16px;background:#26231f;color:#d9d2c8}}\
         header a{{color:#f3efe9;text-decoration:none}} header b{{color:#f3efe9}}\
         main{{padding:12px 16px 40px;overflow-x:auto}}\
         pre{{margin:0}} a{{color:inherit;text-decoration:none}}\
         a:hover{{background:#e8e2d9}} .note{{color:#77706a;margin:10px 0 0}}\
         </style></head><body><header><b>pqbench</b> · \n\
         <a href=\"/\">warehouse</a> · <a href=\"/partitions\">partitions</a> · <a href=\"/insights\">insights</a>\n\
         {crumbs}</header><main><pre>{body}</pre></main></body></html>\n"
    )
}

/// The path from the warehouse to the resource, as links.
fn crumbs(segments: &[&str]) -> String {
    let mut out = String::new();
    for (at, segment) in segments.iter().enumerate() {
        if at % 2 == 0 {
            out.push_str("/ \n");
        } else {
            let label = percent_decode(segment);
            let path = segments[..=at].join("/");
            out.push_str(&format!("<a href=\"/{path}\">{}</a>\n", esc(&label)));
        }
    }
    out
}

/// A version's name without its `/versions/{etag}`: the partition's name,
/// which is the page a row links to.
fn partition_name(version: &str) -> String {
    version
        .split_once("/versions/")
        .map_or(version, |(partition, _)| partition)
        .to_string()
}

/// A note under a table that showed fewer rows than the master holds.
fn note_if_truncated(table: String, shown: usize, total: usize) -> String {
    if shown >= total {
        return table;
    }
    format!(
        "{table}\n<p class=\"note\">the first {shown} of {total}; the JSON API pages the rest</p>\n"
    )
}

/// A percentage-decoded path segment, for a label.
fn percent_decode(text: &str) -> String {
    url::form_urlencoded::parse(text.as_bytes())
        .next()
        .map(|(key, _)| key.into_owned())
        .unwrap_or_else(|| text.to_string())
}

/// Bytes as a short binary unit.
fn size_text(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{bytes} B"),
        _ => format!("{value:.1} {}", UNITS[unit]),
    }
}

/// A monthly cost, or a dash.
fn money(cost: Option<f64>) -> String {
    match cost {
        None => "—".to_string(),
        Some(0.0) => "$0".to_string(),
        Some(cost) if cost < 0.01 => "<$0.01".to_string(),
        Some(cost) if cost >= 1e6 => format!("${:.2}M", cost / 1e6),
        Some(cost) if cost >= 1e3 => format!("${:.1}k", cost / 1e3),
        Some(cost) => format!("${cost:.2}"),
    }
}

/// A millisecond instant as UTC `YYYY-MM-DD HH:MM`, or a dash.
fn when(time: Option<u64>) -> String {
    let Some(time) = time.filter(|time| *time > 0) else {
        return "—".to_string();
    };
    let seconds = time / 1000;
    let (days, rest) = (seconds / 86_400, seconds % 86_400);
    let (year, month, day) = civil_date(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}",
        rest / 3600,
        rest % 3600 / 60
    )
}

/// Days since the Unix epoch as a civil date (Howard Hinnant's algorithm).
fn civil_date(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month + 2) / 5 + 1;
    let month = if month < 10 { month + 3 } else { month - 9 };
    (year + i64::from(month <= 2), month as u32, day as u32)
}

/// A count, or a dash.
fn count(value: Option<u64>) -> String {
    value.map_or_else(|| "—".to_string(), |value| value.to_string())
}

/// A measurement with two decimals, or a dash.
fn number(value: Option<f64>) -> String {
    value.map_or_else(|| "—".to_string(), |value| format!("{value:.2}"))
}

/// The wire spelling of a version's state.
fn state_name(state: State) -> &'static str {
    match state {
        State::Deleted => "DELETED",
        State::Pending => "PENDING",
        State::Pricing => "PRICING",
        State::Priced => "PRICED",
        State::Downloading => "DOWNLOADING",
        State::Downloaded => "DOWNLOADED",
        State::Bytemassing => "BYTEMASSING",
        State::Bytemassed => "BYTEMASSED",
        State::Linting => "LINTING",
        State::UpToDate => "UP_TO_DATE",
        _ => "UNKNOWN",
    }
}

/// A HTML text with the characters that end it escaped.
fn esc(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// What `name` names: a level's stats, a page of one table's versions, or
/// one partition's versions. `None` when it names none of them.
fn read(master: &RwLock<Master>, name: &str, query: &str) -> Result<Option<Value>, String> {
    if name == "partitions" {
        return Ok(Some(recent(master, query)?));
    }
    let segments: Vec<&str> = name.split('/').collect();
    if level(&segments) {
        let records = stats(master, &segments)?;
        return Ok(Some(Value::Array(
            records
                .iter()
                .filter_map(|stats| serde_json::to_value(stats).ok())
                .collect(),
        )));
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
        (
            master.store().to_path_buf(),
            rows.len(),
            select(rows, &page),
        )
    };
    Ok(Some(page_response(&store, total_size, rows, &page)))
}

/// One page of the map whole: every version, the newest found first.
fn recent(master: &RwLock<Master>, query: &str) -> Result<Value, String> {
    let page = Page::parse(query)?;
    let (store, total_size, rows) = {
        let master = lock(master);
        let mut rows = master.list("")?;
        rows.sort_by(|(a_key, a), (b_key, b)| {
            b.create_time
                .cmp(&a.create_time)
                .then_with(|| a_key.cmp(b_key))
        });
        (
            master.store().to_path_buf(),
            rows.len(),
            select(rows, &page),
        )
    };
    Ok(page_response(&store, total_size, rows, &page))
}

/// The page's rows of `rows`, from its offset.
fn select(rows: Vec<(&Key, &PartitionVersion)>, page: &Page) -> Vec<PartitionVersion> {
    rows.into_iter()
        .skip(page.offset)
        .take(page.size)
        .map(|(key, row)| version(key, row))
        .collect()
}

/// A page of versions as the web app serves it, with each row's listing and
/// price.
fn page_response(
    store: &Path,
    total_size: usize,
    rows: Vec<PartitionVersion>,
    page: &Page,
) -> Value {
    let partitions: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut view = view(store, row, &["list-partitions", "price"]);
            trim_files(&mut view);
            view
        })
        .collect();
    let next = page.offset + partitions.len();
    let mut response = json!({ "partitions": partitions, "total_size": total_size });
    if next < total_size {
        response["next_page_token"] = Value::String(next.to_string());
    }
    response
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
fn stats(master: &RwLock<Master>, segments: &[&str]) -> Result<Vec<PartitionStats>, String> {
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
    Ok(rows)
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
