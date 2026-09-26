//! Blackbox tests for planning: console transcripts, commands, expected
//! output, continuations, and directive inheritance.

use docscheck::markdown::parse_source;
use docscheck::plan::Plan;

fn plan(source: &str) -> Plan {
    Plan::from_blocks(&parse_source(source))
}

fn transcript(body: &str) -> String {
    format!("```console run\n{body}\n```\n")
}

#[test]
fn one_prompt_line_per_command() {
    let plan = plan(&transcript("$ ls\n$ pwd"));
    assert_eq!(plan.steps.len(), 2);
    assert_eq!(plan.steps[0].command, "ls");
    assert_eq!(plan.steps[0].line, 2);
    assert_eq!(plan.steps[1].command, "pwd");
    assert_eq!(plan.steps[1].line, 3);
}

#[test]
fn lines_under_a_command_are_its_expected_output() {
    let plan = plan(&transcript("$ echo hi\nhi\n$ echo bye\nbye"));
    assert_eq!(plan.steps[0].expected, vec!["hi".to_owned()]);
    assert_eq!(plan.steps[1].expected, vec!["bye".to_owned()]);
}

#[test]
fn a_command_without_output_has_no_expectation() {
    let plan = plan(&transcript("$ true"));
    assert!(plan.steps[0].expected.is_empty());
    assert!(!plan.steps[0].has_expected());
}

#[test]
fn a_continuation_line_joins_the_command() {
    let plan = plan(&transcript("$ echo one > two\n> three"));
    assert_eq!(plan.steps[0].command, "echo one > two\nthree");
}

#[test]
fn a_blank_line_is_not_expected_output() {
    let plan = plan(&transcript("$ echo hi\nhi\n\n$ echo bye\nbye"));
    assert_eq!(plan.steps[0].expected, vec!["hi".to_owned()]);
}

#[test]
fn a_dot_dot_dot_line_is_plain_output() {
    let plan = plan(&transcript("$ echo hi\nhi\n...\nbye"));
    assert_eq!(
        plan.steps[0].expected,
        vec!["hi".to_owned(), "...".to_owned(), "bye".to_owned()]
    );
}

#[test]
fn a_cd_directive_applies_to_later_steps() {
    let plan = plan(&transcript("# docscheck: cd: sub\n$ echo hi"));
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.steps[0].directory.as_deref(), Some("sub"));
}

#[test]
fn env_directives_accumulate() {
    let plan = plan(&transcript(
        "# docscheck: env: A=1\n# docscheck: env: B=2\n$ echo $A$B",
    ));
    assert_eq!(
        plan.steps[0].variables,
        vec![
            ("A".to_owned(), "1".to_owned()),
            ("B".to_owned(), "2".to_owned()),
        ]
    );
}

#[test]
fn unmarked_blocks_are_skipped_not_planned() {
    let plan = plan("```console\n$ ls\n```\n\n```console run\n$ pwd\n```\n");
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.steps[0].command, "pwd");
    assert_eq!(plan.skipped.len(), 1);
    assert_eq!(plan.skipped[0].line, 1);
}

#[test]
fn a_non_console_language_is_not_run() {
    let plan = plan("```sh run\nls\n```\n");
    assert!(plan.steps.is_empty());
}

#[test]
fn steps_keep_document_order_across_blocks() {
    let plan = plan("```console run\n$ one\n```\n\n```console run\n$ two\n```\n");
    let commands: Vec<&str> = plan
        .steps
        .iter()
        .map(|step| step.command.as_str())
        .collect();
    assert_eq!(commands, ["one", "two"]);
}
