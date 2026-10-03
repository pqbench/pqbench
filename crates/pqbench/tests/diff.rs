use pqbench::{bytemass::MassRow, diff::compare};

fn row(file: &str, group: u32, column: &str, bytes: u64) -> MassRow {
    let mut row = MassRow::default();
    row.uri = file.into();
    row.row_group = group;
    row.column = column.into();
    row.row_count = 10;
    row.compressed_bytes = bytes;
    row
}

#[test]
fn compares_totals_after_rollup_and_counts_rows_once() {
    let left = vec![
        row("a", 0, "nested.a", 20),
        row("a", 1, "nested.a", 30),
        row("a", 0, "nested.b", 50),
        row("a", 0, "gone", 8),
    ];
    let right = vec![row("b", 0, "nested.a", 25), row("b", 0, "new", 0)];
    let result = compare(&left, &right, Some(1)).unwrap();
    let nested = result.iter().find(|row| row.column == "nested").unwrap();
    assert_eq!(nested.left_bytes, Some(100));
    assert_eq!(nested.right_bytes, Some(25));
    assert_eq!(nested.delta_bytes, -75);
    assert_eq!(nested.change_percent, Some(-75.0));
    assert_eq!(nested.left_rows, 10);
    assert_eq!(nested.left_bytes_per_row, Some(10.0));
    let added = result.iter().find(|row| row.column == "new").unwrap();
    assert_eq!(added.left_bytes, None);
    assert_eq!(added.right_bytes, Some(0));
    assert_eq!(added.change_percent, None);
    let gone = result.iter().find(|row| row.column == "gone").unwrap();
    assert_eq!(gone.right_bytes, None);
    assert_eq!(gone.delta_bytes, -8);
}

#[test]
fn rejects_duplicate_chunks_and_overflow_but_handles_wide_signed_deltas() {
    let row = row("a", 0, "id", u64::MAX);
    assert_eq!(
        compare(std::slice::from_ref(&row), &[], None).unwrap()[0].delta_bytes,
        -i128::from(u64::MAX)
    );
    assert!(compare(&[row.clone(), row.clone()], &[], None).is_err());
    let mut other = row.clone();
    other.uri = "b".into();
    assert!(compare(&[row.clone(), other], &[], None).is_err());
    assert!(compare(&[row], &[], Some(0)).is_err());
}
