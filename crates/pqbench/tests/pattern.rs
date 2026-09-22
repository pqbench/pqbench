use pqbench::pattern::{self, Sample};

#[test]
fn matches_unix_globs_on_relative_paths() {
    assert!(pattern::matches("sales/*", "sales/events").unwrap());
    assert!(!pattern::matches("sales/*", "sales/a/b").unwrap());
    assert!(pattern::matches("sales/**", "sales/a/b").unwrap());
    assert!(pattern::matches("*/events", "sales/events").unwrap());
    assert!(pattern::matches("sales*", "sales/events").unwrap());
    assert!(pattern::matches("*events", "sales/events").unwrap());
    assert!(pattern::matches("tmp", "sales/tmp").unwrap());
    assert!(!pattern::matches("tmp", "sales/events").unwrap());
    assert!(pattern::matches("year=2024/**", "year=2024/month=01/part-0.parquet").unwrap());
}

#[test]
fn keep_applies_include_then_exclude() {
    let include = vec!["sales/**".into()];
    let exclude = vec!["tmp".into()];
    assert!(pattern::keep("sales/events", &include, &exclude).unwrap());
    assert!(!pattern::keep("sales/tmp", &include, &exclude).unwrap());
    assert!(!pattern::keep("orders", &include, &exclude).unwrap());
}

#[test]
fn walk_skips_excluded_trees_and_unmatched_prefixes() {
    let include = vec!["sales/**".into()];
    assert!(pattern::walk("sales", &include, &[]).unwrap());
    assert!(!pattern::walk("orders", &include, &[]).unwrap());
    assert!(!pattern::walk("tmp", &[], &["tmp".into()]).unwrap());
    assert!(!pattern::walk("sales/tmp", &include, &["tmp".into()]).unwrap());
}

#[test]
fn sample_keeps_all_every_nth_or_first_n() {
    let items = vec!["a", "b", "c", "d"];
    assert_eq!(
        pattern::select(items.clone(), |item| *item, &[], &[], Sample::ALL).unwrap(),
        ["a", "b", "c", "d"]
    );
    assert_eq!(
        pattern::select(items.clone(), |item| *item, &[], &[], Sample::Every(2)).unwrap(),
        ["a", "c"]
    );
    assert_eq!(
        pattern::select(items, |item| *item, &[], &[], Sample::First(1)).unwrap(),
        ["a"]
    );
}

#[test]
fn sample_rejects_unknown_and_zero() {
    let error = Sample::parse("random").unwrap_err().to_string();
    assert!(error.contains("all, every:N, or first:N"), "{error}");
    assert!(Sample::parse("every:0").is_err());
    assert!(Sample::parse("first:x").is_err());
}
