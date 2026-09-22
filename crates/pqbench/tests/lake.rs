use pqbench::lake;

#[test]
fn discover_names_delta_tables_and_does_not_descend_into_them() {
    let root = tempfile::tempdir().unwrap();
    let events = root.path().join("sales/events");
    let orders = root.path().join("orders");
    std::fs::create_dir_all(events.join("_delta_log")).unwrap();
    std::fs::create_dir_all(events.join("part=a")).unwrap();
    std::fs::create_dir_all(orders.join("_delta_log")).unwrap();
    std::fs::create_dir_all(root.path().join("notes")).unwrap();

    let lake = lake::discover(root.path()).unwrap();
    assert_eq!(lake.kind, "pqbench.lake");
    assert_eq!(lake.version, 1);
    let names: Vec<_> = lake
        .tables
        .iter()
        .map(|table| table.name.as_str())
        .collect();
    assert_eq!(names, ["orders", "sales/events"]);
    assert!(lake.tables.iter().all(|table| table.info.is_none()));
    assert!(lake::render_text(&lake).contains("tables: 2"));
}

#[test]
fn discover_rejects_a_directory_with_no_delta_tables() {
    let root = tempfile::tempdir().unwrap();
    let error = lake::discover(root.path()).unwrap_err().to_string();
    assert!(error.contains("no delta tables"), "{error}");
}
