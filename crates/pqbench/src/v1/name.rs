//! Resource names (AIP-122 paths): build and parse them in one place.
//!
//! A name starts with its collection (`catalogs/{catalog}`, `metastores/{metastore}`,
//! `blobs/{service}/{locator}`, …) and every id segment is percent-encoded to one URI path
//! segment, so a name with a `/` in an id (a locator, a dotted column path)
//! still decodes back to what it names.

use crate::dialect::encode;

/// `metastores/{metastore}`.
#[must_use]
pub fn metastore(metastore: &str) -> String {
    format!("metastores/{}", encode(metastore))
}

/// `catalogs/{catalog}`.
#[must_use]
pub fn catalog(catalog: &str) -> String {
    format!("catalogs/{}", encode(catalog))
}

/// `catalogs/{catalog}/schemas/{schema}`.
#[must_use]
pub fn schema(catalog: &str, schema: &str) -> String {
    format!("{}/schemas/{}", self::catalog(catalog), encode(schema))
}

/// `…/schemas/{schema}/tables/{table}`.
#[must_use]
pub fn table(catalog: &str, schema: &str, table: &str) -> String {
    format!("{}/tables/{}", self::schema(catalog, schema), encode(table))
}

/// `…/tables/{table}/files/{file}`.
#[must_use]
pub fn table_file(catalog: &str, schema: &str, table: &str, file: &str) -> String {
    self::table_file_name(&self::table(catalog, schema, table), file)
}

/// `{table}/files/{file}` — a file under a known table name.
#[must_use]
pub fn table_file_name(table: &str, file: &str) -> String {
    format!("{table}/files/{}", encode(file))
}

/// `{table}/commits/{commit}` — a commit under a known table name.
#[must_use]
pub fn commit_name(table: &str, commit: &str) -> String {
    format!("{table}/commits/{}", encode(commit))
}

/// `…/tables/{table}/commits/{commit}`.
#[must_use]
pub fn commit(catalog: &str, schema: &str, table: &str, commit: &str) -> String {
    format!(
        "{}/commits/{}",
        self::table(catalog, schema, table),
        encode(commit)
    )
}

/// `blobs/{service}/{locator}`.
#[must_use]
pub fn blob(service: &str, locator: &str) -> String {
    format!("blobs/{}/{}", encode(service), encode(locator))
}

/// `…/columns/{column}`.
#[must_use]
pub fn column(service: &str, locator: &str, column: &str) -> String {
    format!(
        "{}/columns/{}",
        self::blob(service, locator),
        encode(column)
    )
}

/// The metastore id in a `metastores/{metastore}` name.
#[must_use]
pub fn parse_metastore(value: &str) -> Option<String> {
    match split(value).as_slice() {
        ["metastores", metastore] => Some(decode(metastore)),
        _ => None,
    }
}

/// The table path, for the table-file name: `…/tables/{table}`.
const TABLE_PATH: &[&str] = &["catalogs", "", "schemas", "", "tables", ""];

/// The ids a name spells: its resource path (`segments`, a literal or `""`
/// for an id), or else the dotted shorthand.
pub(crate) fn ids(value: &str, segments: &[&str], alias: Option<&[&str]>) -> Option<Vec<String>> {
    let parts = split(value);
    if parts.len() == segments.len() {
        let mut ids = Vec::with_capacity(segments.len());
        for (want, got) in segments.iter().zip(&parts) {
            if want.is_empty() {
                ids.push(decode(got));
            } else if *want != &got[..] {
                return None;
            }
        }
        return Some(ids);
    }
    dotted_ids(value, alias?.len())
}

/// The ids the dotted shorthand spells: the first id up to the first dot, the
/// second across the dots, and the rest one each. So `a.b.c.d` is `a`, `b.c`,
/// `d`: a schema id may contain a dot, the other ids may not.
fn dotted_ids(value: &str, placeholders: usize) -> Option<Vec<String>> {
    if value.contains('/') {
        return None;
    }
    let pieces: Vec<&str> = value.split('.').collect();
    if pieces.len() < placeholders || pieces.iter().any(|piece| piece.is_empty()) {
        return None;
    }
    if placeholders == 1 {
        return (pieces.len() == 1).then(|| vec![pieces[0].to_owned()]);
    }
    let tail = placeholders - 2;
    let middle = pieces.len() - tail;
    let mut ids = Vec::with_capacity(placeholders);
    ids.push(pieces[0].to_owned());
    ids.push(pieces[1..middle].join("."));
    ids.extend(pieces[middle..].iter().map(|piece| (*piece).to_owned()));
    Some(ids)
}

/// The canonical name a resource name or its dotted shorthand spells, laid out
/// as `segments` (a literal, or `""` for an id). This is the one call the
/// `Resource` derive makes.
pub(crate) fn pattern_name(
    value: &str,
    segments: &[&str],
    alias: Option<&[&str]>,
) -> Option<String> {
    let ids = ids(value, segments, alias)?;
    let mut name = String::new();
    let mut ids = ids.into_iter();
    for (at, segment) in segments.iter().enumerate() {
        if at > 0 {
            name.push('/');
        }
        if segment.is_empty() {
            name.push_str(&encode(&ids.next()?));
        } else {
            name.push_str(segment);
        }
    }
    Some(name)
}

/// The `local` catalog and schema a path-read table lives in: a storage
/// root no catalog named is still a resource, and these give it one.
const LOCAL: &str = "local";

/// The name a table read by path takes: the `local` catalog and schema
/// around the storage root's last path segment. The root itself stays on
/// the [`Table`] as its `uri`, so nothing is lost.
///
/// [`Table`]: super::Table
#[must_use]
pub fn local_table(uri: &str) -> String {
    let root = uri.trim_end_matches('/');
    let id = root
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(root);
    table(LOCAL, LOCAL, id)
}

/// The `blobs/{service}/{locator}` name a storage URI or local path names.
///
/// The service is the URI scheme (`s3`, `file`, …); a local path (no
/// scheme, or `file://`) is `file`. The locator is the rest, so the name
/// decodes back to what the URI reads.
#[must_use]
pub fn blob_name(uri: &str) -> String {
    let (service, locator) = storage_parts(uri);
    blob(&service, &locator)
}

/// The storage URI a `blobs/{service}/{locator}` name reads. `None` when
/// the name is not a blob name.
#[must_use]
pub fn blob_uri(value: &str) -> Option<String> {
    let (service, locator) = parse_blob(value)?;
    Some(storage_uri(&service, &locator))
}

/// The service and locator in a `blobs/{service}/{locator}` name.
#[must_use]
pub fn parse_blob(value: &str) -> Option<(String, String)> {
    match split(value).as_slice() {
        ["blobs", service, locator] => Some((decode(service), decode(locator))),
        _ => None,
    }
}

/// The table name and file path in a `…/tables/{table}/files/{file}` name.
#[must_use]
pub fn parse_table_file(value: &str) -> Option<(String, String)> {
    let (table, file) = value.rsplit_once("/files/")?;
    ids(table, TABLE_PATH, None)?;
    Some((table.to_string(), decode(file)))
}

/// Split a storage URI or local path into a blob's service and locator.
fn storage_parts(uri: &str) -> (String, String) {
    if let Some(locator) = uri.strip_prefix("file://") {
        return ("file".to_string(), locator.to_string());
    }
    match uri.split_once("://") {
        Some((scheme, locator)) if !scheme.is_empty() => (scheme.to_string(), locator.to_string()),
        _ => ("file".to_string(), uri.to_string()),
    }
}

/// The URI a blob's service and locator read. A `file` blob is its bare
/// path, which the local readers accept.
fn storage_uri(service: &str, locator: &str) -> String {
    if service == "file" {
        locator.to_string()
    } else {
        format!("{service}://{locator}")
    }
}

fn split(value: &str) -> Vec<&str> {
    value.split('/').collect()
}

/// Percent-decode one path segment back to its id.
pub(crate) fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                out.push(high * 16 + low);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(byte: u8) -> Option<u8> {
    (byte as char).to_digit(16).map(|value| value as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::{Catalog, Schema, Table};

    #[test]
    fn names_round_trip_through_their_parsers() {
        assert_eq!(
            Catalog::parse_name(&catalog("dbx_samples")),
            Some("dbx_samples".to_string())
        );
        assert_eq!(
            Schema::parse_name(&schema("dbx_samples", "nyctaxi")),
            Some(("dbx_samples".to_string(), "nyctaxi".to_string()))
        );
        assert_eq!(
            Table::parse_name(&table("dbx_samples", "nyctaxi", "trips")),
            Some((
                "dbx_samples".to_string(),
                "nyctaxi".to_string(),
                "trips".to_string()
            ))
        );
    }

    #[test]
    fn an_id_with_a_slash_stays_one_segment() {
        let value = table("c", "s", "t/x");
        assert!(value.contains("%2F"), "{value}");
        assert_eq!(
            Table::parse_name(&value),
            Some(("c".to_string(), "s".to_string(), "t/x".to_string()))
        );
    }

    #[test]
    fn a_name_of_another_shape_parses_as_none() {
        assert_eq!(Table::parse_name("catalogs/dbx_samples"), None);
        assert_eq!(Schema::parse_name("catalogs/dbx_samples"), None);
    }

    #[test]
    fn a_blob_name_round_trips_to_the_storage_uri() {
        for uri in [
            "s3://lakehouse/unity/events/part-0.parquet",
            "/tmp/x.parquet",
            "file:///tmp/y.parquet",
        ] {
            let blob = blob_name(uri);
            assert!(blob.starts_with("blobs/"), "{blob}");
            let read = blob_uri(&blob).unwrap();
            assert!(
                read == uri || uri.strip_prefix("file://") == Some(read.as_str()),
                "{uri} -> {blob} -> {read}"
            );
        }
        assert_eq!(
            parse_blob("blobs/s3/bucket%2Fkey"),
            Some(("s3".to_string(), "bucket/key".to_string()))
        );
    }

    #[test]
    fn a_path_read_table_gets_a_local_name() {
        assert_eq!(
            local_table("/tmp/lake/events"),
            "catalogs/local/schemas/local/tables/events"
        );
        assert_eq!(
            Table::parse_name(&local_table("s3://bucket/events/")),
            Some((
                "local".to_string(),
                "local".to_string(),
                "events".to_string()
            ))
        );
    }

    #[test]
    fn a_table_file_name_parses_to_its_table() {
        let value = table_file("c", "s", "t", "part-0.parquet");
        assert_eq!(
            parse_table_file(&value),
            Some((
                "catalogs/c/schemas/s/tables/t".to_string(),
                "part-0.parquet".to_string()
            ))
        );
        assert_eq!(parse_table_file("catalogs/c/schemas/s/tables/t"), None);
    }

    #[test]
    fn a_bare_catalog_id_parses() {
        assert_eq!(
            Catalog::parse_name("dbx_samples"),
            Some("dbx_samples".to_string())
        );
        assert_eq!(Catalog::parse_name("a.b"), None);
        assert_eq!(Catalog::parse_name("catalogs/a"), Some("a".to_string()));
    }

    #[test]
    fn a_dotted_schema_parses() {
        assert_eq!(
            Schema::parse_name("a.b"),
            Some(("a".to_string(), "b".to_string()))
        );
        assert_eq!(
            Schema::parse_name("a.b.c"),
            Some(("a".to_string(), "b.c".to_string()))
        );
        assert_eq!(Schema::parse_name("a"), None);
    }

    #[test]
    fn a_dotted_table_parses() {
        assert_eq!(
            Table::parse_name("a.b.c"),
            Some(("a".to_string(), "b".to_string(), "c".to_string()))
        );
        assert_eq!(
            Table::parse_name("a.b.c.d"),
            Some(("a".to_string(), "b.c".to_string(), "d".to_string()))
        );
        assert_eq!(Table::parse_name("a.b"), None);
    }

    #[test]
    fn a_pattern_renders_the_canonical_name() {
        const TABLE: &[&str] = &["catalogs", "", "schemas", "", "tables", ""];
        const ALIAS: &[&str] = &["", "", ""];
        assert_eq!(
            pattern_name("a.b.c", TABLE, Some(ALIAS)),
            Some(table("a", "b", "c"))
        );
        assert_eq!(
            pattern_name("a.b.c.d", TABLE, Some(ALIAS)),
            Some(table("a", "b.c", "d"))
        );
        assert_eq!(
            pattern_name("catalogs/a/schemas/b/tables/c", TABLE, None),
            Some(table("a", "b", "c"))
        );
        assert_eq!(pattern_name("a.b.c", TABLE, None), None);
        assert_eq!(pattern_name("metastores/x", TABLE, None), None);
    }
}
