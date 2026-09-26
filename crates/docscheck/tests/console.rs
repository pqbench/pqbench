//! Blackbox tests for the console-fence gate: a `pqbench` command example must
//! live in a `console` block so it can be a test.

use docscheck::{invokes_pqbench, markdown::parse_source, pqbench_outside_console};

#[test]
fn a_bare_pqbench_command_is_an_invocation() {
    assert!(invokes_pqbench("pqbench bytemass a.parquet\n"));
}

#[test]
fn a_pipeline_stage_counts() {
    assert!(invokes_pqbench(
        "lake x | pqbench table | pqbench bytemass\n"
    ));
    assert!(invokes_pqbench("xargs pqbench bytemass\n"));
}

#[test]
fn an_assignment_before_the_command_still_counts() {
    assert!(invokes_pqbench(
        "AWS_PROFILE=analytics pqbench bytemass s3://b/t/p.parquet\n"
    ));
}

#[test]
fn data_and_prose_are_not_invocations() {
    assert!(!invokes_pqbench(
        "{\"kind\":\"pqbench.bytemass\",\"event\":\"begin\"}\n"
    ));
    assert!(!invokes_pqbench("import pqbench\n"));
    assert!(!invokes_pqbench(
        "docker run --rm pqbench/pqbench:latest bytemass /tmp/f.parquet\n"
    ));
    assert!(!invokes_pqbench("# run pqbench later\n"));
}

#[test]
fn a_sh_block_with_a_pqbench_command_is_reported() {
    let source = "```sh\npqbench bytemass a.parquet\n```\n\n\
                  ```console run\n$ pqbench bytemass a.parquet\n```\n";
    let blocks = parse_source(source);
    let offenders = pqbench_outside_console(&blocks);
    assert_eq!(offenders.len(), 1);
    assert_eq!(offenders[0].language(), Some("sh"));
}

#[test]
fn diag_actual_lines() {
    for line in [
        "pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report",
        "AWS_PROFILE=analytics pqbench bytemass s3://bucket/table/part-0.parquet",
    ] {
        eprintln!("{line:?} -> {}", invokes_pqbench(line));
    }
}

#[test]
fn diag_docs_readme() {
    let src = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/README.md"))
        .unwrap();
    let blocks = parse_source(&src);
    for b in &blocks {
        eprintln!(
            "lang={:?} line={} pq={} :: {:?}",
            b.language(),
            b.line,
            invokes_pqbench(&b.body),
            b.body.lines().next()
        );
    }
}
