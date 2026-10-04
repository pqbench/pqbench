## Summary

<!-- What changed and why; reference the issue (`Refs #N`). -->

## Checks

<!--
The boxes open checked: every check below is required for the change (where
the change touches it) and is verified before the PR opens. The pre-commit
hook runs `make check` on each commit; CI re-runs the gate and the heavy
cycle. Uncheck a box only when it genuinely does not apply, and say why. See
CONTRIBUTING.md for the full loop. The certification line at the bottom names
the commit the checks ran against; the `PR checklist` workflow fails while it
is missing or stale.
-->

- [x] **Behavior** — the change works against the real endpoint/stand, not only
  in tests; behavior changes kept separate from tidyings.
- [x] **Blackbox tests** — public API only, offline, sub-second in `make test`;
  the changed behavior is covered.
- [x] **E2E** — `make dbx-e2e` for the catalog/schema/table commands (Unity
  REST and Iceberg REST dialects), `make lakehouse` for the stand commands;
  both where applicable.
- [x] **Tidyings** — Kent Beck's tidyings, each structure-only and its own
  revertible commit, run before the behavior change.
- [x] **rust-skills** — err-, num-, serde-, api-/own-, name- rules checked.
- [x] **QA** — `make check` green (fmt-check, check-docs, clippy `-D warnings`,
  isolation, lfs-check, test); `make check CARGO_FEATURES=--all-features`;
  `make lakehouse`.
- [x] **aipnaming** — `cargo run -p aipnaming-cli -- crates/` clean; exceptions
  are inline `aipnaming: allow(...)` with a reason.
- [x] **Perf** — `perf stat` (+ `perf record` for the hot path) on the command;
  wall time and bytes read for remote; obvious latency flagged, numbers
  recorded in `docs/performance-audit.md`.
- [x] **Docs** — `docs/cli.md` (+ `docs/delta.md` / `docs/iceberg.md` /
  `docs/docker.md` / stand README as needed).

<!--
The certification line is per commit: after the last push, re-run the checks
and replace <head sha> with the head commit SHA (the PR's top commit). A push
moves the head, the old line stops matching, and the `PR checklist` workflow
goes red until the line is updated — a stale description cannot be merged.
-->

Above checks are true for the commit "<head sha>"
