use std::path::Path;

use pqbench::lake;
use pqbench::table::{self, TableFormat};

fn names(lake: &pqbench::lake::Lake) -> Vec<&str> {
    lake.tables
        .iter()
        .map(|table| table.name.as_str())
        .collect()
}

fn uri(dir: &Path) -> String {
    dir.to_str().unwrap().to_string()
}

fn warehouse() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let events = root.path().join("sales/events");
    let orders = root.path().join("orders");
    let reviews = root.path().join("catalog/reviews");
    std::fs::create_dir_all(events.join("_delta_log")).unwrap();
    std::fs::create_dir_all(events.join("part=a")).unwrap();
    std::fs::create_dir_all(orders.join("_delta_log")).unwrap();
    std::fs::create_dir_all(reviews.join("metadata")).unwrap();
    std::fs::create_dir_all(reviews.join("data")).unwrap();
    std::fs::write(reviews.join("metadata/v1.metadata.json"), "{}").unwrap();
    std::fs::write(reviews.join("metadata/version-hint.text"), "1").unwrap();
    std::fs::create_dir_all(root.path().join("notes")).unwrap();
    // Nested markers under a table must not be listed.
    std::fs::create_dir_all(reviews.join("data/nested/_delta_log")).unwrap();
    std::fs::create_dir_all(reviews.join("data/nested/metadata")).unwrap();
    std::fs::write(reviews.join("data/nested/metadata/v1.metadata.json"), "{}").unwrap();
    // `metadata/*.metadata.json` is one path component; nested JSON is not Iceberg.
    std::fs::create_dir_all(root.path().join("notes/metadata/sub")).unwrap();
    std::fs::write(
        root.path().join("notes/metadata/sub/v1.metadata.json"),
        "{}",
    )
    .unwrap();
    // UniForm: Delta wins; do not emit the same path twice or walk its data.
    let uniform = root.path().join("uniform");
    std::fs::create_dir_all(uniform.join("_delta_log")).unwrap();
    std::fs::create_dir_all(uniform.join("metadata")).unwrap();
    std::fs::write(uniform.join("metadata/v1.metadata.json"), "{}").unwrap();
    root
}

#[tokio::test]
async fn discover_names_delta_and_iceberg_tables_and_does_not_descend_into_them() {
    let root = warehouse();
    let lake = lake::discover(&uri(root.path()), &Default::default())
        .await
        .unwrap();
    assert_eq!(lake.kind, "pqbench.lake");
    assert_eq!(lake.version, 1);
    assert_eq!(
        names(&lake),
        ["catalog/reviews", "orders", "sales/events", "uniform"]
    );
    assert!(lake.tables.iter().all(|table| table.info.is_none()));
    assert!(lake::render_text(&lake).contains("tables: 4"));
}

#[tokio::test]
async fn discover_rejects_a_directory_with_no_tables() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("notes/metadata/sub")).unwrap();
    std::fs::write(
        root.path().join("notes/metadata/sub/v1.metadata.json"),
        "{}",
    )
    .unwrap();
    let error = lake::discover(&uri(root.path()), &Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no tables"), "{error}");
}

#[tokio::test]
async fn discover_at_stops_at_max_depth() {
    let root = warehouse();
    let lake = lake::discover_bounded(&uri(root.path()), &Default::default(), Some(1), |_| true)
        .await
        .unwrap();
    assert_eq!(names(&lake), ["orders", "uniform"]);
}

#[tokio::test]
async fn discover_prunes_prefixes_that_cannot_yield_a_table() {
    let root = warehouse();
    let lake = lake::discover_bounded(&uri(root.path()), &Default::default(), None, |name| {
        name.starts_with("sales")
    })
    .await
    .unwrap();
    assert_eq!(names(&lake), ["sales/events"]);
}

#[tokio::test]
async fn discover_walk_root_does_not_repeat_the_prefix_component() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("events/_delta_log")).unwrap();
    let lake = lake::discover(&uri(root.path()), &Default::default())
        .await
        .unwrap();
    assert_eq!(names(&lake), ["events"]);
}

#[tokio::test]
async fn discover_uri_file_matches_a_bare_path() {
    let root = warehouse();
    let path = lake::discover(&uri(root.path()), &Default::default())
        .await
        .unwrap();
    let file = url::Url::from_directory_path(root.path()).unwrap();
    let listed = lake::discover(file.as_str(), &Default::default())
        .await
        .unwrap();
    assert_eq!(names(&listed), names(&path));
}

#[tokio::test]
async fn discover_uri_decodes_a_file_uri() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("my lake");
    std::fs::create_dir_all(root.join("orders/_delta_log")).unwrap();
    let file = url::Url::from_directory_path(&root).unwrap();
    assert!(file.as_str().contains("%20") || root.to_str().unwrap().contains(' '));
    let lake = lake::discover(file.as_str(), &Default::default())
        .await
        .unwrap();
    assert_eq!(names(&lake), ["orders"]);
}

#[tokio::test]
async fn listed_tables_are_detectable() {
    let root = warehouse();
    let lake = lake::discover(&uri(root.path()), &Default::default())
        .await
        .unwrap();
    for table in &lake.tables {
        let format = table::detect(&table.uri, &Default::default())
            .await
            .unwrap();
        assert_ne!(format, TableFormat::UNSPECIFIED, "{}", table.uri);
    }
}

#[cfg(not(feature = "aws"))]
#[tokio::test]
async fn discover_uri_names_the_missing_aws_feature() {
    let error = lake::discover("s3://bucket/warehouse", &Default::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("aws"), "{error}");
}
