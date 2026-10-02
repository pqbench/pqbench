use pqbench::table::{FileSelection, TableFile};
fn file(path: &str, bytes: u64, year: &str) -> TableFile {
    let mut file = TableFile::new(path, path, bytes);
    file.partition_values
        .insert("year".into(), Some(year.into()));
    file
}
#[test]
fn filters_before_sampling_and_breaks_median_ties_by_path() {
    let files = vec![
        file("z", 90, "2024"),
        file("b", 20, "2024"),
        file("a", 10, "2024"),
        file("d", 1, "2023"),
        file("c", 30, "2024"),
    ];
    let selection = FileSelection {
        partitions: [("year".into(), Some("2024".into()))].into(),
        sample: "median:2".into(),
        ..Default::default()
    };
    let result = selection.select_files(files.clone()).unwrap();
    assert_eq!(
        result.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );
    let selection = FileSelection {
        include: vec!["[abc]".into()],
        exclude: vec!["b".into()],
        sample: "every:2".into(),
        ..Default::default()
    };
    assert_eq!(selection.select_files(files).unwrap()[0].path, "a");
}
#[test]
fn rejects_bad_policies_and_unknown_sizes() {
    for sample in ["first:0", "every:0", "median:0", "random:2"] {
        assert!(FileSelection {
            sample: sample.into(),
            ..Default::default()
        }
        .select_files(Vec::new())
        .is_err());
    }
    assert!(FileSelection {
        sample: "median:1".into(),
        ..Default::default()
    }
    .select_files(vec![file("a", 0, "2024")])
    .is_err());
}
