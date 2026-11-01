use pqbench::diff::compare;
use pqbench::v1;

/// One measured column: `bytes` on-disk over `values` values, under `file`.
fn mass(file: &str, column: &str, bytes: u64, values: u64) -> v1::Column {
    let blob = v1::name::blob_name(file);
    let (service, locator) = v1::name::parse_blob(&blob).expect("a blob name");
    v1::Column {
        name: v1::name::column(&service, &locator, column),
        mass: Some(v1::Bytemass {
            compressed_bytes: Some(bytes),
            value_count: Some(values),
            ..v1::Bytemass::default()
        }),
        ..v1::Column::default()
    }
}

#[test]
fn compares_totals_after_rollup_and_keeps_absent_columns() {
    let left = vec![
        mass("a", "nested.a", 20, 10),
        mass("a", "nested.b", 80, 10),
        mass("a", "gone", 8, 10),
    ];
    let right = vec![mass("b", "nested.a", 25, 10), mass("b", "new", 0, 10)];
    let result = compare(&left, &right, Some(1)).unwrap();
    let nested = result.iter().find(|row| row.column == "nested").unwrap();
    assert_eq!(nested.left_bytes, Some(100));
    assert_eq!(nested.right_bytes, Some(25));
    assert_eq!(nested.delta_bytes, -75);
    assert_eq!(nested.change_percent, Some(-75.0));
    assert_eq!(nested.left_values, 20);
    assert_eq!(nested.left_bytes_per_value, Some(5.0));
    let added = result.iter().find(|row| row.column == "new").unwrap();
    assert_eq!(added.left_bytes, None);
    assert_eq!(added.right_bytes, Some(0));
    assert_eq!(added.change_percent, None);
    let gone = result.iter().find(|row| row.column == "gone").unwrap();
    assert_eq!(gone.right_bytes, None);
    assert_eq!(gone.delta_bytes, -8);
}

#[test]
fn compares_leaf_paths_when_no_depth_is_given() {
    let left = vec![mass("a", "nested.a", 20, 10), mass("a", "nested.b", 80, 10)];
    let result = compare(&left, &[], None).unwrap();
    let mut columns: Vec<_> = result.iter().map(|row| row.column.as_str()).collect();
    columns.sort_unstable();
    assert_eq!(columns, ["nested.a", "nested.b"]);
}

#[test]
fn rejects_duplicates_depth_zero_and_non_bytemass_names_but_handles_wide_deltas() {
    let row = mass("a", "id", u64::MAX, 10);
    assert_eq!(
        compare(std::slice::from_ref(&row), &[], None).unwrap()[0].delta_bytes,
        -i128::from(u64::MAX)
    );
    assert!(compare(&[row.clone(), row.clone()], &[], None).is_err());
    assert!(compare(std::slice::from_ref(&row), &[], Some(0)).is_err());

    let mut unnamed = row;
    unnamed.name = "not-a-bytemass".into();
    assert!(compare(&[unnamed], &[], None).is_err());
}
