//! Blackbox tests for the generator: the Rust source it emits.

use docscheck::generate;
use docscheck::markdown::parse_source;

fn gen(source: &str) -> String {
    generate("doc.md", &parse_source(source))
}

fn transcript(body: &str) -> String {
    format!("```console run\n{body}\n```\n")
}

#[test]
fn emits_a_test_named_after_the_heading() {
    let out = gen(&format!("## lz\n\n{}", transcript("$ echo hi")));
    assert!(out.contains("#[test]\nfn lz()"));
    assert!(out.contains("support::run("));
}

#[test]
fn a_block_without_a_heading_falls_back_to_its_line() {
    let out = gen(&transcript("$ echo hi"));
    assert!(out.contains("fn line_1()"));
}

#[test]
fn duplicate_headings_get_distinct_names() {
    let out = gen(&format!(
        "## run\n\n{}\n## run\n\n{}",
        transcript("$ one"),
        transcript("$ two"),
    ));
    assert!(out.contains("fn run()"));
    assert!(out.contains("fn run_2()"));
}

#[test]
fn an_unmarked_block_emits_nothing() {
    let out = gen("```console\n$ echo hi\n```\n");
    assert!(!out.contains("#[test]"));
    assert!(out.contains("No runnable blocks"));
}

#[test]
fn the_header_names_the_source_and_the_support_module() {
    let out = gen(&transcript("$ one"));
    assert!(out.contains("// @generated"));
    assert!(out.contains("#[path = \"gen_support.rs\"]"));
}

#[test]
fn the_command_is_embedded_as_a_string_literal() {
    let out = gen(&transcript("$ pqbench lz examples/quickstart.parquet"));
    assert!(out.contains(r#""pqbench lz examples/quickstart.parquet""#));
    assert!(out.contains("None"));
}

#[test]
fn expected_output_becomes_a_slice_of_lines() {
    let out = gen(&transcript("$ echo hi\nhi\nthere"));
    assert!(out.contains(r#"Some(&["hi", "there"])"#));
}

#[test]
fn an_elision_line_is_emitted_verbatim() {
    let out = gen(&transcript("$ echo hi\nhi\n...\nbye"));
    assert!(out.contains(r#"Some(&["hi", "...", "bye"])"#));
}

#[test]
fn a_command_with_quotes_uses_a_raw_string() {
    let out = gen(&transcript("$ pqbench viz -o report && echo \"done\""));
    assert!(out.contains("r#\""));
    assert!(out.contains("echo \"done\""));
}

#[test]
fn a_multi_command_block_becomes_one_test_with_several_calls() {
    let out = gen(&format!("## demo\n\n{}", transcript("$ one\n$ two")));
    assert_eq!(out.matches("#[test]").count(), 1);
    assert_eq!(out.matches("support::run(").count(), 2);
}

#[test]
fn a_piped_command_stays_a_single_call() {
    let out = gen(&format!("## pipe\n\n{}", transcript("$ a | b")));
    assert_eq!(out.matches("support::run(").count(), 1);
}

#[test]
fn a_heading_slug_is_snake_case() {
    let out = gen(&format!("## One Parquet file\n\n{}", transcript("$ one")));
    assert!(out.contains("fn one_parquet_file()"));
}

#[test]
fn a_feature_word_gates_the_test() {
    let out = gen("## t\n\n```console run delta\n$ pqbench table x\n```\n");
    assert!(out.contains("#[cfg(feature = \"delta\")]"));
}

#[test]
fn several_feature_words_gate_on_all_of_them() {
    let out = gen("## t\n\n```console run delta aws\n$ pqbench table x\n```\n");
    assert!(out.contains("#[cfg(all(feature = \"delta\", feature = \"aws\"))]"));
}

#[test]
fn no_feature_word_emits_no_cfg() {
    let out = gen(&format!("## t\n\n{}", transcript("$ pqbench bytemass x")));
    assert!(!out.contains("#[cfg("));
}
