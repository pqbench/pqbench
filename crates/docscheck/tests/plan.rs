//! Blackbox tests for planning: splitting a block into steps, continuations,
//! and directive inheritance.

use docscheck::markdown::parse_source;
use docscheck::plan::Plan;

fn plan(source: &str) -> Plan {
    Plan::from_blocks(&parse_source(source))
}

#[test]
fn one_line_per_command() {
    let plan = plan("```sh run\nls\npwd\n```\n");
    assert_eq!(plan.steps.len(), 2);
    assert_eq!(plan.steps[0].command, "ls");
    assert_eq!(plan.steps[0].line, 2);
    assert_eq!(plan.steps[1].command, "pwd");
    assert_eq!(plan.steps[1].line, 3);
}

#[test]
fn a_backslash_continuation_joins_lines() {
    let plan = plan("```sh run\necho one \\\n  two\n```\n");
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.steps[0].command, "echo one \\\n  two");
    assert_eq!(plan.steps[0].line, 2);
}

#[test]
fn a_trailing_pipe_joins_the_next_line() {
    let plan = plan("```sh run\npqbench table x |\n  pqbench bytemass\n```\n");
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(
        plan.steps[0].command,
        "pqbench table x |\n  pqbench bytemass"
    );
}

#[test]
fn a_pipe_at_the_end_of_a_joined_line_keeps_joining() {
    let plan = plan("```sh run\na \\\nb |\nc\n```\n");
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.steps[0].command, "a \\\nb |\nc");
}

#[test]
fn comments_are_dropped_but_not_step_lines() {
    let plan = plan("```sh run\n# note\nls\n```\n");
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.steps[0].command, "ls");
    assert_eq!(plan.steps[0].line, 3);
}

#[test]
fn blank_lines_are_skipped() {
    let plan = plan("```sh run\nls\n\n\npwd\n```\n");
    assert_eq!(plan.steps.len(), 2);
    assert_eq!(plan.steps[1].line, 5);
}

#[test]
fn a_cd_directive_applies_to_later_steps() {
    let plan = plan("```sh run\n# docscheck: cd: sub\necho hi\n```\n");
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.steps[0].directory.as_deref(), Some("sub"));
}

#[test]
fn env_directives_accumulate() {
    let plan = plan("```sh run\n# docscheck: env: A=1\n# docscheck: env: B=2\necho $A$B\n```\n");
    assert_eq!(
        plan.steps[0].environment,
        vec![
            ("A".to_owned(), "1".to_owned()),
            ("B".to_owned(), "2".to_owned()),
        ]
    );
}

#[test]
fn unmarked_blocks_are_skipped_not_planned() {
    let plan = plan("```sh\nls\n```\n\n```sh run\npwd\n```\n");
    assert_eq!(plan.steps.len(), 1);
    assert_eq!(plan.steps[0].command, "pwd");
    assert_eq!(plan.skipped.len(), 1);
    assert_eq!(plan.skipped[0].line, 1);
}

#[test]
fn steps_keep_document_order_across_blocks() {
    let plan = plan("```sh run\none\n```\n\n```sh run\ntwo\n```\n");
    let commands: Vec<&str> = plan
        .steps
        .iter()
        .map(|step| step.command.as_str())
        .collect();
    assert_eq!(commands, ["one", "two"]);
}
