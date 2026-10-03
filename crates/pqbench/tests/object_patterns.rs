use pqbench::third_party::object_store::api::{expand_glob, KeyPattern};
#[test]
fn key_patterns_keep_path_boundaries_and_literal_prefixes() {
    let pattern = KeyPattern::parse("table/day=2024/part-?.parquet").unwrap();
    assert_eq!(pattern.prefix(), "table/day=2024");
    assert!(pattern.matches("table/day=2024/part-a.parquet"));
    assert!(!pattern.matches("table/day=2024/sub/part-a.parquet"));
    assert!(!pattern.matches("table/day=2024/part-ab.parquet"));
    let pattern = KeyPattern::parse("table/**/*.parquet").unwrap();
    assert!(pattern.matches("table/a.parquet"));
    assert!(pattern.matches("table/sub/b.parquet"));
    assert!(!pattern.matches("other/b.parquet"));
    assert!(KeyPattern::parse("table/[1]/*.parquet")
        .unwrap()
        .matches("table/[1]/a.parquet"));
}
#[tokio::test]
async fn exact_uris_and_percent_escaped_wildcards_never_list() {
    for uri in ["s3://bucket/path.parquet", "s3://bucket/literal%2A.parquet"] {
        assert_eq!(expand_glob(uri, &[]).await.unwrap(), [uri]);
    }
}
