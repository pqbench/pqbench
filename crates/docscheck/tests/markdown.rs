//! Blackbox tests for the tree-sitter front end: blocks, info strings, and
//! the run marker.

use docscheck::markdown::parse_source;
use docscheck::{BlockInfo, Directive};

#[test]
fn extracts_fenced_blocks_in_order() {
    let source = "# Title\n\n```sh\nls\n```\n\nprose\n\n```json\n{}\n```\n";
    let blocks = parse_source(source);
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].language(), Some("sh"));
    assert_eq!(blocks[0].line, 3);
    assert_eq!(blocks[0].body, "ls\n");
    assert_eq!(blocks[1].language(), Some("json"));
    assert_eq!(blocks[1].line, 9);
}

#[test]
fn the_run_word_marks_a_block_runnable() {
    let source = "```sh run\necho hi\n```\n";
    let blocks = parse_source(source);
    assert_eq!(blocks.len(), 1);
    assert!(blocks[0].is_runnable());
    assert_eq!(blocks[0].info.language.as_deref(), Some("sh"));
    assert!(blocks[0].info.options.is_empty());
}

#[test]
fn an_unmarked_block_is_not_runnable() {
    let source = "```sh\necho hi\n```\n";
    let blocks = parse_source(source);
    assert!(!blocks[0].is_runnable());
}

#[test]
fn no_run_wins_over_run() {
    let info = BlockInfo::parse("sh run no-run");
    assert!(!info.run);
}

#[test]
fn a_bare_fence_has_no_language() {
    let source = "```\nplain\n```\n";
    let blocks = parse_source(source);
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].language(), None);
    assert!(!blocks[0].is_runnable());
}

#[test]
fn body_line_points_at_the_first_code_line() {
    let source = "prefix\n\n```sh run\necho hi\n```\n";
    let blocks = parse_source(source);
    assert_eq!(blocks[0].line, 3);
    assert_eq!(blocks[0].body_line, 4);
}

#[test]
fn indented_fences_nested_in_lists_are_found() {
    let source = "- step\n\n  ```sh run\n  echo nested\n  ```\n";
    let blocks = parse_source(source);
    assert_eq!(blocks.len(), 1);
    assert!(blocks[0].is_runnable());
    assert!(blocks[0].body.contains("echo nested"));
}

#[test]
fn a_document_without_fences_has_no_blocks() {
    assert!(parse_source("# just prose\n").is_empty());
}

#[test]
fn a_block_records_its_nearest_heading() {
    let source = "# Title\n\n## lz\n\n```sh run\necho hi\n```\n";
    let blocks = parse_source(source);
    assert_eq!(blocks[0].heading.as_deref(), Some("lz"));
}

#[test]
fn a_later_heading_replaces_the_earlier_one() {
    let source = "## first\n\n```sh run\none\n```\n\n## second\n\n```sh run\ntwo\n```\n";
    let blocks = parse_source(source);
    assert_eq!(blocks[0].heading.as_deref(), Some("first"));
    assert_eq!(blocks[1].heading.as_deref(), Some("second"));
}

#[test]
fn blocks_before_any_heading_have_no_heading() {
    let source = "```sh run\none\n```\n";
    let blocks = parse_source(source);
    assert_eq!(blocks[0].heading, None);
}

#[test]
fn parses_a_cd_directive() {
    let directive = Directive::parse("# docscheck: cd: docker/e2e-lakehouse");
    assert_eq!(
        directive,
        Some(Directive::Directory("docker/e2e-lakehouse".to_owned()))
    );
}

#[test]
fn parses_an_env_directive() {
    let directive =
        Directive::parse("  # docscheck: env: PQB_ENDPOINT=https://example.cloud.databricks.com");
    assert_eq!(
        directive,
        Some(Directive::Environment {
            name: "PQB_ENDPOINT".to_owned(),
            value: "https://example.cloud.databricks.com".to_owned(),
        })
    );
}

#[test]
fn an_ordinary_comment_is_not_a_directive() {
    assert_eq!(Directive::parse("# just a note"), None);
    assert_eq!(Directive::parse("echo hi"), None);
}
