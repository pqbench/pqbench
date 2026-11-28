//! Shared helpers for the walk's commands: endpoint URLs and the backend
//! vocabulary they report.
//!
//! The table format is declared by the caller (`PQB_TABLE_FORMAT`); nothing
//! here probes the endpoint. Iceberg REST endpoints already name the catalog
//! base (`{root}/v1` or `{root}/v1/{prefix}`), so the commands append
//! `/namespaces…` directly.

use crate::v1::CatalogType;

// A name segment and a query value encode the same way, and a name is only
// encoded segments, so the one encoder lives with the names.
pub(crate) use pqbench_model_v1::name::encode;

/// The workspace root an endpoint of either dialect names: an Iceberg REST
/// catalog base was built from it (`{root}/iceberg-rest/v1[/{prefix}]`), and a
/// Unity endpoint may already name the REST root. The OAuth token route
/// (`{root}/oidc/v1/token`) hangs off this root, not off either catalog base.
pub(crate) fn workspace_root(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    let root = endpoint
        .split_once("/iceberg-rest/")
        .map_or(endpoint, |(root, _)| root);
    root.trim_end_matches("/api/2.1/unity-catalog").to_string()
}

/// The Unity REST root, whether or not the endpoint already names it.
pub(crate) fn api_root(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    if endpoint.ends_with("/api/2.1/unity-catalog") {
        endpoint.to_string()
    } else {
        format!("{endpoint}/api/2.1/unity-catalog")
    }
}

/// The Iceberg REST base: the endpoint without its trailing slash.
pub(crate) fn iceberg_root(endpoint: &str) -> &str {
    endpoint.trim_end_matches('/')
}

/// The Unity REST root of an endpoint of either dialect: an Iceberg REST
/// catalog base names the root it was built from
/// (`{root}/iceberg-rest/v1[/{prefix}]`).
pub(crate) fn unity_root(endpoint: &str) -> String {
    endpoint
        .split_once("/iceberg-rest/")
        .map_or_else(|| api_root(endpoint), |(root, _)| api_root(root))
}

/// A percent-encoded Iceberg namespace: parts joined by the unit separator.
pub(crate) fn iceberg_namespace(schema: &str) -> String {
    encode(&schema.split('.').collect::<Vec<_>>().join("\u{1f}"))
}

/// The catalog kind the endpoint reports (`MANAGED_CATALOG`, …) as the proto
/// enum. A kind this build does not name is `None`, not an error.
pub(crate) fn catalog_type(kind: &str) -> Option<CatalogType> {
    match kind {
        "MANAGED_CATALOG" => Some(CatalogType::Managed),
        "FOREIGN_CATALOG" => Some(CatalogType::Foreign),
        "SYSTEM_CATALOG" => Some(CatalogType::System),
        _ => None,
    }
}
