use std::process::Command;

fn help(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_pqbench"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn root_help_covers_auth_documents_and_format_skills() {
    let stdout = help(&["--help"]);
    assert!(stdout.contains("pqbench.table"), "{stdout}");
    assert!(stdout.contains("AWS_SESSION_TOKEN"), "{stdout}");
    assert!(stdout.contains("DATABRICKS_TOKEN"), "{stdout}");
    assert!(stdout.contains("parquet.apache.org"), "{stdout}");
    assert!(stdout.contains("docs/cli.md"), "{stdout}");
}

#[test]
fn table_help_stays_local_and_points_at_neighbors() {
    let stdout = help(&["table", "--help"]);
    assert!(stdout.contains("Delta"), "{stdout}");
    assert!(stdout.contains("Iceberg"), "{stdout}");
    assert!(stdout.contains("pqbench bytemass --help"), "{stdout}");
    assert!(stdout.contains("pqbench --help"), "{stdout}");
    assert!(
        !stdout.contains("parquet.apache.org"),
        "format skills belong on pqbench --help, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("AWS_SESSION_TOKEN"),
        "object-store auth belongs on pqbench --help, got:\n{stdout}"
    );
}

#[test]
fn bytemass_help_points_at_table_and_viz() {
    let stdout = help(&["bytemass", "--help"]);
    assert!(stdout.contains("every:N"), "{stdout}");
    assert!(stdout.contains("--indexes"), "{stdout}");
    assert!(stdout.contains("pqbench table --help"), "{stdout}");
    assert!(stdout.contains("pqbench viz --help"), "{stdout}");
    assert!(stdout.contains("pqbench --help"), "{stdout}");
    assert!(!stdout.contains("parquet.apache.org"), "{stdout}");
}

#[test]
fn profile_help_points_at_dump() {
    let stdout = help(&["profile", "--help"]);
    assert!(stdout.contains("--dependencies"), "{stdout}");
    assert!(stdout.contains("pqbench dump --help"), "{stdout}");
    assert!(stdout.contains("pqbench --help"), "{stdout}");
    assert!(!stdout.contains("parquet.apache.org"), "{stdout}");
}

#[test]
fn lake_help_names_catalog_env_and_points_at_table() {
    let stdout = help(&["lake", "--help"]);
    assert!(stdout.contains("DATABRICKS_HOST"), "{stdout}");
    assert!(stdout.contains("pqbench table --help"), "{stdout}");
    assert!(stdout.contains("pqbench --help"), "{stdout}");
    assert!(!stdout.contains("parquet.apache.org"), "{stdout}");
}
