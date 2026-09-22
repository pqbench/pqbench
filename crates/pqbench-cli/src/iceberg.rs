//! List tables from an Iceberg REST catalog.
//!
//! The catalog is the Iceberg REST fixture dialect used by the lakehouse
//! stand: `GET /v1/config`, `GET /v1/namespaces`, `GET /v1/namespaces/{ns}/tables`,
//! then `GET /v1/namespaces/{ns}/tables/{table}` for the metadata location.
//! Namespace loops run concurrently up to `--concurrency`. `--include` /
//! `--exclude` prune the walk when the leading name is a literal.
//! https://iceberg.apache.org/docs/latest/rest-catalog-spec/

use std::collections::{BTreeMap, BTreeSet};

use pqbench::lake::LakeTable;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::catalog::{self, PAGE_CAP};
use crate::document::LakeSource;
use crate::filter::NameFilter;
use crate::CliError;

#[derive(Deserialize)]
struct NamespacesPage {
    namespaces: Vec<Vec<String>>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct TablesPage {
    identifiers: Vec<Identifier>,
    #[serde(default, rename = "next-page-token", alias = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct Identifier {
    name: String,
}

#[derive(Deserialize)]
struct LoadedTable {
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

/// List Iceberg tables, sending each as soon as its namespace page arrives.
pub(crate) async fn list_tables(
    source: &LakeSource,
    filter: &NameFilter,
    concurrency: usize,
    mut on_table: impl FnMut(LakeTable) -> Result<(), CliError>,
) -> Result<usize, CliError> {
    let root = source.catalog_endpoint()?.trim_end_matches('/').to_string();
    let token = source.catalog_token();
    let env = source.storage_env();
    let namespaces = namespaces_to_list(&root, token.as_deref(), filter)?;
    let (tx, mut rx) = mpsc::unbounded_channel::<Result<LakeTable, String>>();
    let mut set: JoinSet<Result<(), String>> = JoinSet::new();
    let mut tables = 0usize;

    for namespace in namespaces {
        while set.len() >= concurrency {
            if let Some(done) = set.join_next().await {
                done.map_err(|error| error.to_string())??;
            }
            drain_tables(&mut rx, &mut on_table, &mut tables)?;
        }
        let root = root.clone();
        let token = token.clone();
        let env = env.clone();
        let filter = filter.clone();
        let tx = tx.clone();
        set.spawn_blocking(move || {
            tables_in(&root, token.as_deref(), &namespace, &env, &filter, &tx)
        });
    }
    drop(tx);
    while let Some(done) = set.join_next().await {
        done.map_err(|error| error.to_string())??;
        drain_tables(&mut rx, &mut on_table, &mut tables)?;
    }
    while let Some(item) = rx.recv().await {
        on_table(item?)?;
        tables += 1;
    }
    if tables == 0 {
        return Err("catalog listed no Iceberg tables".into());
    }
    Ok(tables)
}

fn drain_tables(
    rx: &mut mpsc::UnboundedReceiver<Result<LakeTable, String>>,
    on_table: &mut impl FnMut(LakeTable) -> Result<(), CliError>,
    tables: &mut usize,
) -> Result<(), CliError> {
    while let Ok(item) = rx.try_recv() {
        on_table(item?)?;
        *tables += 1;
    }
    Ok(())
}

fn namespaces_to_list(
    root: &str,
    token: Option<&str>,
    filter: &NameFilter,
) -> Result<Vec<Vec<String>>, CliError> {
    let mut namespaces = Vec::new();
    for page in pages::<NamespacesPage>(root, token, "/v1/namespaces", |page| {
        page_token(&page.next_page_token)
    })? {
        for parts in page.namespaces {
            if parts.is_empty() || parts.iter().any(|part| part.is_empty()) {
                return Err("catalog /v1/namespaces listed a nameless namespace".into());
            }
            if filter.keeps_prefix(&parts.join(".")) {
                namespaces.push(parts);
            }
        }
    }
    Ok(namespaces)
}

fn tables_in(
    root: &str,
    token: Option<&str>,
    namespace: &[String],
    env: &BTreeMap<String, String>,
    filter: &NameFilter,
    tx: &mpsc::UnboundedSender<Result<LakeTable, String>>,
) -> Result<(), String> {
    let encoded = namespace_path(namespace);
    for page in pages::<TablesPage>(
        root,
        token,
        &format!("/v1/namespaces/{encoded}/tables"),
        |page| page_token(&page.next_page_token),
    )
    .map_err(|error| error.to_string())?
    {
        for item in page.identifiers {
            if item.name.is_empty() {
                return Err(format!(
                    "catalog listed a nameless table in namespace {}",
                    namespace.join(".")
                ));
            }
            let table = load_table(root, token, namespace, &item.name, env)
                .map_err(|error| error.to_string())?;
            if filter.keeps(&table.name) {
                tx.send(Ok(table))
                    .map_err(|_| "lake output closed".to_string())?;
            }
        }
    }
    Ok(())
}

fn load_table(
    root: &str,
    token: Option<&str>,
    namespace: &[String],
    name: &str,
    env: &BTreeMap<String, String>,
) -> Result<LakeTable, CliError> {
    let url = format!(
        "{root}/v1/namespaces/{}/tables/{}",
        namespace_path(namespace),
        catalog::encode(name)
    );
    let loaded: LoadedTable = catalog::get_json(&url, token)?;
    if loaded.metadata_location.is_empty() {
        return Err(format!(
            "Iceberg table {}.{} is missing metadata-location",
            namespace.join("."),
            name
        )
        .into());
    }
    let full_name = namespace
        .iter()
        .cloned()
        .chain(std::iter::once(name.to_string()))
        .collect::<Vec<_>>()
        .join(".");
    Ok(LakeTable {
        name: full_name,
        uri: loaded.metadata_location,
        env: env.clone(),
        info: None,
    })
}

fn namespace_path(namespace: &[String]) -> String {
    catalog::encode(&namespace.join("\u{1f}"))
}

fn page_token(token: &Option<String>) -> Option<String> {
    token
        .as_deref()
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

fn pages<P: for<'de> Deserialize<'de>>(
    root: &str,
    token: Option<&str>,
    path: &str,
    next: fn(&P) -> Option<String>,
) -> Result<Vec<P>, CliError> {
    let mut page_token: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut pages = Vec::new();
    loop {
        if pages.len() >= PAGE_CAP {
            return Err(format!("catalog listed more than {PAGE_CAP} pages at {path}").into());
        }
        let mut url = format!("{root}{path}");
        if let Some(token) = &page_token {
            url.push_str("?pageToken=");
            url.push_str(&catalog::encode(token));
        }
        let page: P = catalog::get_json(&url, token)?;
        let next = next(&page);
        pages.push(page);
        let Some(next) = next else {
            return Ok(pages);
        };
        if !seen.insert(next.clone()) {
            return Err(format!("catalog repeated page token at {path}").into());
        }
        page_token = Some(next);
    }
}
