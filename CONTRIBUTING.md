# Contributing to pqbench

## Getting started

Requires Rust 1.94.1+ (the `--all-features` build; the default feature set's
declared minimum is 1.91.1) and [git-lfs](https://git-lfs.com) (for the LFS test
fixtures). Clone, build, and try the bundled sample:

```sh
git clone https://github.com/pqbench/pqbench.git
cd pqbench
make check   # fmt-check + check-docs + clippy -D warnings + isolation + lfs-check + test + test-code-analysis
cargo run -p pqbench-cli -- bytemass examples/quickstart.parquet
```

`examples/quickstart.parquet` is a small smoke sample — its numbers aren't
benchmark-grade. The real datasets live in `data/samples/` (Git LFS); `make
samples` verifies they are fetched. For using the CLI, see the README quick
start.

## The loop

Every change follows the same loop. Steps 2–4 are the checks you run before
committing; each tidy in step 3 is its own commit.

1. **Change** — make the behavior change.
2. **Test** — run the gate.
3. **Tidy** — apply Kent Beck's tidyings, each as a separate commit.
4. **Check conventions** — rust-skills rules and naming.
5. **Update AGENTS.md** — if the loop or a rule changed.
6. **Commit** — only when asked.

## 1. Change

Make the change in the library (`crates/pqbench`) or the CLI (`crates/pqbench-cli`).
Keep behavior changes separate from tidyings.

## 2. Test

```
make fmt
make check        # = fmt-check + check-docs + clippy -D warnings + isolation + lfs-check + test + test-code-analysis
```

- `make check` is the gate and runs automatically in the pre-commit hook.
- `make check-fast` is the iteration gate: fmt-check + check-docs + `cargo
  check --all-targets`, with no codegen or links. The full `make check` is for
  before pushing. Heavy cargo runs are serialized by `make` (one lock across
  profiles and worktrees) and capped by `[build] jobs`; do not run a second
  build in parallel. A bare `cargo` in a worktree should go through `make cargo
  CMD="…"`; `make link-target` points the worktree's `target/` at the shared
  one.
- **Integration tests are one target per crate.** `tests/it.rs` wires each
  `tests/*.rs` file in with `#[path]` and `mod`; Cargo builds only `it` (one
  binary per file used to link the whole graph once per file). Add the module
  line when you add a test file, and run one file with
  `cargo test -p <crate> --test it -- <module>`.
- `make check-python` builds the data-only wheel with maturin and smoke-tests
  the installed binary. It is separate from the gate because it compiles every
  storage backend. CI runs it on every pull request.
- **Documented commands are tests.** The `console` blocks in `README.md`,
  `docs/`, and `skills/` are generated into `crates/pqbench-cli/tests/gen_*.rs`
  by `docscheck` and run by `make test`. A block runs only when its fence
  carries `run`; a `$ ` line is a command and the lines under it are its
  expected output:
  ````md
  ```console run
  $ pqb table-file examples/quickstart.parquet | pqb bytemass | pqb fmt
  column  codec         encodings                 bytes  values
  ------  ------------  ------------------------  -----  ------
  id      UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    102       8
  year    UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY     68       8
  ```
  ````
  The output is matched token by token; a documented token that starts with `±`
  (a measured speed) or is a run of dashes (a table separator) matches any
  actual token, so a value that varies is marked rather than filtered in the
  shell. `docscheck` gates two things before generating: a `pqbench`
  example must be in a `console` fence, and a transcript must document the human
  table, not NDJSON. A `{"kind":"pqbench` output line fails unless the fence
  opts in with ` ```console run json ` (the `json` word marks a machine-output
  example; it is not a cargo feature). Add a feature word
  (` ```console run delta `) to gate the test on a cargo feature. After editing
  a doc block, run `make sync-docs`; `check-docs` (part of `make check`, hence
  the pre-commit hook) fails when the generated files are stale. Never edit
  `tests/gen_*.rs` by hand.
- Feature sets are exercised through the same targets:
  `make check CARGO_FEATURES=--all-features` (or `CARGO_FEATURES="--features aws"`,
  `CARGO_FEATURES="--features delta"`). CI runs one clippy over
  `--all-features` and a test leg per set, with default and all-features also on
  arm64.
- The network e2e (`redset`) is `#[ignore]`d so local `make test` stays offline
  and sub-second. CI runs it on every PR via
  `make test CARGO_FEATURES="--features aws" TEST_FLAGS=--include-ignored`.
- `make samples` verifies the committed open parquet datasets in `data/samples/`
  (Git LFS; run `git lfs pull` after a clone that skipped LFS).
- `make code-analysis` prints rust-code-analysis metrics for `crates/` (the
  CLI's own metrics tree). It needs `rust-code-analysis-cli` on `PATH`; `RCA`
  and `CODE_ANALYSIS_PATHS` override the binary and inputs. It is
  **report-only** — not part of `make check`. On a PR, CI analyzes only the
  changed Rust files against the base revision: `scripts/code_analysis.py`
  reports functions that are new or worsened over cyclomatic 15 as advisory
  notices, posts a sticky comment with those findings and their base→head
  deltas, and logs the raw diff. It measures a function's own complexity, not
  the sum that folds in its closures, and excludes tests, generated sources,
  and file-level metrics. This is a review prompt, not a gate: extracting a
  helper lowers the maximum without reducing total complexity. On a push it
  rolls the whole tree up for the job summary.
- Tests are **blackbox** (observable behavior through the public API), unit
  level, sub-second, no network, no external build steps.
- During implementation, prefer `cargo check` for fast feedback. The repository
  uses `sccache` automatically when installed; inspect it with `make cache-stats`.
  Use release builds only when measuring benchmark behavior or release artifacts.

## 3. Tidy (Tidy First?)

- Tidyings are **structure-only** — they must never change observable output.
- Do them in tiny, reversible steps, each as its **own commit**.
- Common ones here: `guard-clauses`, `dead-code`, `normalize-symmetries`,
  `reading-order`, `cohesion-order`, `move-declaration-init-together`,
  `explaining-variables`, `explaining-constants`, `chunk-statements`,
  `extract-helper`, `delete-redundant-comments`.
- See AGENTS.md *Tidyings* for the full list; run them before behavior changes.
- These tidyings are from Kent Beck's *Tidy First?* (O'Reilly, 2023) — [newsletter](https://newsletter.kentbeck.com/).

## 4. Check conventions

### Rust conventions (rust-skills)

The workspace follows the rust-skills rule set. The rules that bite most here:

- `err-` — `Result` over panicking; `?` and `From` for propagation; no
  `unwrap()`/`expect()` in production; error messages lowercase, no trailing
  punctuation.
- `num-` — no narrowing `as` casts; use `TryFrom` with `unwrap_or`; `total_cmp`
  for float ordering.
- `serde-` — `skip_serializing_if` for empty fields; rename to match the output
  convention.
- `api-`/`own-` — accept `&[T]`/`&str`, not `&Vec`/`&String`; expose only the
  crate's own types (no third-party type leaks in the public API).
- `name-` — see naming below.

### Naming (AIP — but prefer Rust casing)

Follow AGENTS.md *Naming guidelines (AIP)*, with one caveat: where AIP is
directional and conflicts with idiomatic Rust, **Rust casing wins**.

- Types `UpperCamelCase`; functions `VerbNoun` (no `get_` prefix); modules,
  fields, and functions `snake_case`.
- Fields are **nouns, not verbs**; booleans omit the verb prefix (`dictionary`,
  not `is_dictionary`; AIP-140); keep a **single root** for a family
  (`compress`/`decompress`, not a mix of verb and noun forms).
- Units spelled out, never ambiguous abbreviations: `megabytes_per_second`, not
  `mbps`; `_estimate` for a measurement; `_duration`/`_durations` for spans.
- Ranges use `first`/`last`; no bare `time`.
- Enums: prefer `UpperCamelCase` variants (idiomatic Rust), even though AIP-126
  says `UPPER_SNAKE_CASE` — that rule is directional.

`make check` also runs the `aipnaming` linter's tests and the code-analysis
reporter's tests; when editing its word data or rules, see
`crates/aipnaming/README.md`.

## 5. Documentation

The documentation follows [Diátaxis](https://diataxis.fr/): tutorials teach,
how-to guides solve a task, reference states the facts, and explanation gives
the background. The site's sections are the four forms:

- **Getting started** — the tutorial: install, first measurement, first treemap.
- **Guides** — task-oriented walkthroughs (showcases, demos, Delta, Iceberg,
  Databricks authentication, performance).
- **Reference** — the generated CLI reference, the document shapes,
  authentication, configuration, distribution, and the Python wheel.
- **Concepts** — the data model and the architecture.

The CLI reference is **generated from the clap command tree** with
`clap-markdown` by the `docsgen` tool (`crates/docsgen`); do not edit
`docs/reference/cli.md` by hand. `make sync-docs`
writes it, and `make check-docs` fails when it is stale. The prose it renders
comes from the doc comments and `crates/pqbench-cli/src/help.rs`, so change the
help text there and regenerate.

Style sources:

- [Diátaxis](https://diataxis.fr/) — the four forms and their boundaries.
- [Google developer documentation style
  guide](https://developers.google.com/style) — voice, tense, code samples,
  headings, and inclusive language.
- [Microsoft Writing Style
  Guide](https://learn.microsoft.com/style-guide/welcome/) — plain language and
  bias-free wording.
- [Write the Docs](https://www.writethedocs.org/guide/) — docs as code, and
  documentation in the development workflow.
- [The Good Docs Project](https://thegooddocsproject.dev/) — templates and
  content patterns.
- Jared Bhatti et al., *Docs for Developers: An Engineer's Field Guide to
  Technical Writing* (Apress, 2021) — the developer-advocate workflow.

## 6. Update AGENTS.md

`AGENTS.md` is the source of truth for agents. If you change the gate, the loop,
or a convention, update `AGENTS.md` so the next agent follows it. Also record
every file you edit in your reply as a plain path reference (see AGENTS.md
*Reporting edits*).

## 7. Commit

- **Commit only when the user asks.**
- Each tidy is its own commit; keep behavior changes separate.
- The pre-commit hook runs `make check`, so the gate must be green.

## 8. Contributions & licensing

- **License:** `MIT OR Apache-2.0` (dual). See `LICENSE-MIT` and `LICENSE-APACHE`.
- **Inbound = outbound:** by contributing, your work is licensed to the project
  under that dual license (Apache-2.0 §5 *Submission of Contributions*).

## Pointers

- **AGENTS.md** — the full rules: principles, naming, tidyings, Unix philosophy,
  first principles, references.
- **Dependency-light & fast compile** — keep the dependency tree small (avoid
  heavy deps; verify nothing pulls in `arrow`), prefer the gnu target, and reuse
  cached artifacts (shared `CARGO_TARGET_DIR`) and incremental builds. Compile
  time and the sub-second test suite are hard requirements, not suggestions.
- **No ad-hoc workarounds** — if the environment is wrong, fix it (the flatpak
  SDK toolchain / its manifest), don't PATH-hack, bundle broken artifacts, or
  hardcode paths.
- **README / docs** — follow the [Documentation](#5-documentation) section:
  Diátaxis, the generated CLI reference, and the style sources.
