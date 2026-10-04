## Summary

<!-- What changed and why; reference the issue (`Refs #N`). -->

## Checks

<!--
The boxes open checked: every check below is required for the change and is
verified before the PR opens (the pre-commit hook runs `make check` on each
commit; CI re-runs the gate and the heavy cycle). Uncheck a box only when it
genuinely does not apply, and say why. See CONTRIBUTING.md for the full loop.
The certification line at the bottom names the commit the checks ran against;
the `PR checklist` workflow fails while it is missing or stale.
-->

- [x] **Tests** — blackbox tests cover the behavior change; `make check` green
  (fmt-check, check-docs, clippy `-D warnings`, isolation, lfs-check, test);
  the feature sets the change touches (`CARGO_FEATURES=...`) exercised.
- [x] **QA** — `make check CARGO_FEATURES=--all-features`; the suites beyond
  the gate that the change touches (`make check-python`, `make dbx-e2e`,
  `make lakehouse`); `make sync-docs` after editing a documented command.
- [x] **Tidyings** — Kent Beck's tidyings, applied before the behavior change;
  structure-only, each its own revertible commit (Tidy First?).
- [x] **aipnaming** — `cargo run -p aipnaming-cli -- crates/` clean; exceptions
  are inline `aipnaming: allow(...)` with a reason.
- [x] **rust-skills** — err-, num-, serde-, api-/own-, name- rules checked:
  `Result` over panicking, no narrowing `as` casts, no third-party types in the
  public API.

<!--
The certification line is per commit: after the last push, re-run the checks
and replace <head sha> with the head commit SHA (the PR's top commit). A push
moves the head, the old line stops matching, and the `PR checklist` workflow
goes red until the line is updated — a stale description cannot be merged.
-->

Above checks are true for the commit "<head sha>"
