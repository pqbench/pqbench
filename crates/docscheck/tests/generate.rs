//! Blackbox tests for the generator: the Rust source it emits.

use docscheck::generate;
use docscheck::markdown::parse_source;

fn gen(source: &str) -> String {
    generate("doc.md", &parse_source(source))
}

#[test]
fn emits_a_test_named_after_the_heading() {
    let out = gen("## lz\n\n```sh run\necho hi\n```\n");
    assert!(out.contains("#[test]\nfn lz()"));
    assert!(out.contains("support::run("));
}

#[test]
fn a_block_without_a_heading_falls_back_to_its_line() {
    let out = gen("```sh run\necho hi\n```\n");
    assert!(out.contains("fn line_1()"));
}

#[test]
fn duplicate_headings_get_distinct_names() {
    let out = gen("## run\n\n```sh run\none\n```\n\n## run\n\n```sh run\ntwo\n```\n");
    assert!(out.contains("fn run()"));
    assert!(out.contains("fn run_2()"));
}

#[test]
fn an_unmarked_block_emits_nothing() {
    let out = gen("```sh\necho hi\n```\n");
    assert!(!out.contains("#[test]"));
    assert!(out.contains("No runnable blocks"));
}

#[test]
fn the_header_names_the_source_and_the_support_module() {
    let out = gen("```sh run\none\n```\n");
    assert!(out.contains("// @generated"));
    assert!(out.contains("#[path = \"gen_support.rs\"]"));
}

#[test]
fn the_command_is_embedded_as_a_string_literal() {
    let out = gen("```sh run\npqbench lz examples/quickstart.parquet\n```\n");
    assert!(out.contains(r#""pqbench lz examples/quickstart.parquet""#));
}

#[test]
fn a_command_with_quotes_uses_a_raw_string() {
    let out = gen("```sh run\npqbench viz -o report && echo \"done\"\n```\n");
    assert!(out.contains("r#\""));
    assert!(out.contains("echo \"done\""));
}

#[test]
fn a_multi_line_block_becomes_one_test_with_several_calls() {
    let out = gen("## demo\n\n```sh run\none\ntwo\n```\n");
    assert_eq!(out.matches("#[test]").count(), 1);
    assert_eq!(out.matches("support::run(").count(), 2);
}

#[test]
fn a_piped_command_stays_a_single_call() {
    let out = gen("## pipe\n\n```sh run\na | b\n```\n");
    assert_eq!(out.matches("support::run(").count(), 1);
}

#[test]
fn a_heading_slug_is_snake_case() {
    let out = gen("## One Parquet file\n\n```sh run\none\n```\n");
    assert!(out.contains("fn one_parquet_file()"));
}

#[test]
fn a_feature_word_gates_the_test() {
    let out = gen("## t\n\n```sh run delta\npqbench table x\n```\n");
    assert!(out.contains("#[cfg(feature = \"delta\")]"));
}

#[test]
fn several_feature_words_gate_on_all_of_them() {
    let out = gen("## t\n\n```sh run delta aws\npqbench table x\n```\n");
    assert!(out.contains("#[cfg(all(feature = \"delta\", feature = \"aws\"))]"));
}

#[test]
fn no_feature_word_emits_no_cfg() {
    let out = gen("## t\n\n```sh run\npqbench bytemass x\n```\n");
    assert!(!out.contains("#[cfg("));
}
