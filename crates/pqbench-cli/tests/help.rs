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
    assert!(stdout.contains("TableFile"), "{stdout}");
    assert!(stdout.contains("AWS_SESSION_TOKEN"), "{stdout}");
    assert!(stdout.contains("parquet.apache.org"), "{stdout}");
    assert!(stdout.contains("docs/cli.md"), "{stdout}");
}

#[test]
fn help_shows_the_short_names() {
    let stdout = help(&["--help"]);
    for alias in ["fmt", "ls", "rl"] {
        assert!(stdout.contains(&format!("[alias: {alias}]")), "{stdout}");
    }
}

#[test]
fn get_help_stays_local_and_points_at_neighbors() {
    let stdout = help(&["get", "--help"]);
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
fn bytemass_help_points_at_list_and_viz() {
    let stdout = help(&["bytemass", "--help"]);
    assert!(stdout.contains("pqbench list --help"), "{stdout}");
    assert!(stdout.contains("pqbench viz --help"), "{stdout}");
    assert!(stdout.contains("pqbench --help"), "{stdout}");
    assert!(!stdout.contains("parquet.apache.org"), "{stdout}");
}

#[test]
fn list_help_points_at_get_and_bytemass() {
    let stdout = help(&["list", "--help"]);
    assert!(stdout.contains("catalogs"), "{stdout}");
    assert!(stdout.contains("pqbench get --help"), "{stdout}");
    assert!(stdout.contains("pqbench bytemass --help"), "{stdout}");
    assert!(!stdout.contains("parquet.apache.org"), "{stdout}");
}

#[test]
fn skill_help_points_at_profile() {
    let stdout = help(&["skill", "--help"]);
    assert!(stdout.contains("parquet-advisor"), "{stdout}");
    assert!(stdout.contains("recipes"), "{stdout}");
    assert!(stdout.contains("pqbench profile --help"), "{stdout}");
    assert!(!stdout.contains("parquet.apache.org"), "{stdout}");
}
