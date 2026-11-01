# AGENTS.md

Working rules for agents in the pqbench repository.

## Communication (no slop)

The reply is the shortest text that carries the answer.

- No preamble, no restating the request, no closing summary, no offer to
  continue. Answer, then stop.
- A few plain lines beat a structured document: no headers, tables, or bullet
  lists for an answer that is a paragraph or two.
- Facts, paths, commands; `file:line` when the location matters.
- Write plainly, for a reader who knows the product but not your reasoning.
  Name the concrete thing (`v1::TableFile`), not an invented abstraction: no
  "layer", "adapter", "superset", "reversible" unless the code or the docs
  already use the word. Say "the old file record has `path` and `uri`; the v1
  one has `name` and `blob`", not "the two families differ by layer".
- One question at most, and only when the answer changes what happens next.

## Principles

- **No ad-hoc workarounds.** If the environment is unsuitable, fix the
  environment itself; avoid PATH hacks, musl/custom-linker overrides, bundled
  broken artifacts, or per-machine config. No hardcoded paths in scripts.
- **Fast compilation time.** Compile time matters. Keep the dependency tree
  small; prefer the default (gnu) target; reuse cached artifacts (shared
  `CARGO_TARGET_DIR`) and incremental builds; avoid heavy new dependencies
  (e.g. anything that drags in a full LLVM rebuild).
- **Bounded builds.** The all-features test binaries link at multi-GB RSS
  each: keep cargo's jobs capped, take the `flock` in `make`, and use
  `make check-fast` (type-check, no links) while iterating. Parallel links
  exhaust a 14 GB machine and take the editor down with them.
- **Fast tests.** The test suite must stay fast (sub-second). Keep tests at
  unit level; no network, no external build steps. Every change must pass
  `make check`.
- **Blackbox tests only.** Tests verify observable behavior through the public
  API (round-trips, sizes, error/panic contracts, cross-checks against
  references) — never internal implementation details (no asserting on internal
  names, ranges, or helper functions that a change to the same code would
  trivially keep passing). A test that can only fail because its own
  implementation is wrong is useless; it must be able to fail because the
  behavior is wrong.
- **Docs** — follow developer-advocate best practices.

## Naming guidelines (AIP)

Follow Google AIP conventions for the public surface (AIP-121/122/190/136/140/141/142/145/126):

- **Methods/functions** — `VerbNoun` (AIP-136): `Get/List/Create/Update/Delete`
  for resources; custom methods are imperative verbs (`render`, `aggregate`,
  `measure`, `bench_file`, `read_masses`). No `get` prefix.
- **Types** — `CamelCase` nouns (`ReportRow`, `MassNode`, `Estimate`, `ChunkResult`).
- **Fields/struct members** — `snake_case` nouns, not verbs; booleans omit the
  verb prefix (`dictionary`, `json` — not `is_dictionary`; AIP-140); no
  field/type name collision (`ReportRow.rows`).
- **Units** are spelled out, never ambiguous abbreviations (AIP-141):
  `megabytes_per_second`, not `mbps`/`mb_per_s`; `_duration`/`_durations`, not
  `_times`; `_estimate` for a measurement (not `_measurement`).
- **Enums** — `UPPER_SNAKE_CASE` variants; `_UNSPECIFIED`/`_UNKNOWN` zero value
  (AIP-126).
- **Ranges/intervals** — `first`/`last` (`LevelRange { first_level, last_level }`),
  not `min`/`max` (AIP-145).
- **Time fields** — `_time` for instants, `_duration`/`_durations` for spans
  (AIP-142); no bare `time`.

`crates/aipnaming` enforces these; its own docs (`crates/aipnaming/README.md`)
describe how.

## Tidyings (Tidy First?)

- Apply Kent Beck's tidyings to make a change safe and reviewable, and run them
  *before* behavior changes; keep each tidy as its own revertible commit.
- Common tidyings used here: `explaining-variables` (name a computed value used
  more than once), `extract-helper` (a block with obvious purpose and limited
  interaction), `guard-clauses`, `cohesion-order`, `reading-order`,
  `normalize-symmetries`, `chunk-statements`, `delete-redundant-comments`,
  `dead-code`, `move-declaration-init-together`.
- Tidyings are structure, not behavior — they must not change observable output.

## Unix philosophy

Grounded in McIlroy's maxims, Pike's notes, and the classic rules:

- **Do one thing well.** To do a new job, build afresh rather than complicate an
  old program with new features (`bytemass` measures byte masses; a browser
  treemap page — d3 from a CDN — is the downstream visualizer, not the tool).
- **Composition over monoliths.** Expect every program's output to become
  another's input. Read/write simple, stream-oriented text (CSV/JSON); don't
  clutter output with extraneous info; don't require interactive input; avoid
  stringently columnar or binary formats. A clean interface lets you replace
  either end without disturbing the other.
- **Modularity.** Simple parts with clean interfaces keep global complexity
  local, so you can upgrade a part without breaking the whole.
- **Separation of policy from mechanism.** Layered modules
  (raw → analytics → presentation) let each concern evolve and be tested
  independently; policy changes shouldn't destabilize the mechanisms.
- **Clarity over cleverness.** Code is read by humans; buy performance only with
  clarity, never by trading away readability.
- **Simplicity; parsimony.** Design for simplicity, add complexity only where
  you must; don't write a big program until nothing smaller will do.
- **Transparency & discoverability.** Design so a program can be seen to work:
  simple input/output formats, and debugging/monitoring designed in, not bolted
  on. Robustness follows from transparency + simplicity.
- **Rule of Representation.** Fold knowledge into data so program logic can be
  stupid and robust; prefer data structures over intricate procedures.
- **Least Surprise.** In interfaces, do the least surprising thing; follow
  existing conventions rather than gratuitous novelty.
- **Silence.** When a program has nothing surprising to say, it should say
  nothing; keep output clean so the next tool can pick out what it needs.
- **Repair / fail noisily.** Be liberal in what you accept, conservative in what
  you send; when you must fail, fail loudly and as early as possible (clear
  errors, never silent corruption).
- **Economy.** Programmer time is expensive; conserve it in preference to
  machine time — build tools rather than do tedious work by hand.
- **Generation.** Avoid hand-hacking; write programs to write programs when it
  raises the abstraction (`make samples` fetches test data, `make check` gates).
- **Prototype before polishing.** Make it run, then right, then fast; avoid
  premature optimization, which usually costs both speed and clarity.
- **Extensibility.** Make data formats self-describing/versioned so they can
  evolve forward without breaking readers.
- **Diversity.** Distrust claims of "one true way"; keep options open.

## Low coupling · domain isolation · small dep tree

- **Low coupling** — decouple commands and modules as much as possible; a change
  shouldn't spread into unrelated code.
- **Domain isolation** — isolate third-party APIs and domain code into their own
  modules (like the parquet helpers); the CLI is a thin wrapper over the library.
- **Small dep tree** — keep heavy dependencies optional and off the default build;
  compile and test time are hard constraints.

Reference (history): https://github.com/pqbench/pqbench/pull/6

## First principles

Reason from first principles (what is true / the data model) rather than by
analogy, convention, or precedent:

- Derive the design from the data model and the problem, not from precedent.
- Question assumptions; choose the simplest thing that is correct.
- When metadata already has the answer, read metadata — don't decode pages
  (`read_masses` reads only the footer, so it works on any parquet, compressed
  or not).
- When two paths conflict, prefer the one that keeps the tool testable,
  dependency-light, and composable.

## Build / check workflow

- Format: `make fmt`
- Gate (pre-commit): `make check` = `fmt-check` + `check-docs` +
  `clippy -D warnings` + `isolation` + `lfs-check` + `test` +
  `test-code-analysis`.
  `make check-python` builds the data-only wheel from `crates/pqbench-cli`
  with maturin and smoke-tests the installed binary; CI runs it, the local gate
  does not (it compiles every storage backend).
- **Worktrees share the main checkout's `target/`.** The Makefile derives it
  from `git rev-parse --git-common-dir`, so a linked worktree reuses the cached
  artifacts instead of recompiling the tree for the pre-commit gate.
  `CARGO_BUILD_JOBS` defaults to 2 because the test binaries link several GB
  each; raise it only when the machine has the memory. When the gate already
  passed, `git commit --no-verify` is acceptable.
- Docs: the CLI reference is generated from the clap tree with `clap-markdown`;
  `make sync-docs` writes `docs/reference/cli.md`, and `check-docs` fails when
  it is stale. The `console` transcripts in the Markdown are generated into
  `crates/pqbench-cli/tests/gen_*.rs` and run by `make test`. Edit a doc block,
  run `make sync-docs`; `check-docs` (part of `make check`, hence the pre-commit
  hook) fails when the generated files are stale. A block runs only when its
  fence carries `run` (e.g. ` ```console run `, or
  ` ```console run delta ` to gate the test on a cargo feature); a `$ ` line is
  a command and the lines under it are its expected output, matched token by
  token. A documented token stands for a value that varies when it starts with
  `±` (a measured speed — see the `lz`/`compression` tables) or is a run of
  dashes (a separator whose width follows the data); either matches any actual
  token, so mark a varying number rather than filter it in the shell.
  Two gates run before generation. A `pqbench` example outside a `console` fence
  fails, so a command example cannot silently go untested. A transcript that
  documents an NDJSON line (`{"kind":"pqbench`) also fails — the docs show the
  human table; a block whose example is *about* the machine output opts in with
  ` ```console run json ` (the `json` word is not a cargo feature).
- Features: everything builds with `--all-features` — the Makefile default, so
  `make check` and the pre-commit hook use it without any flag. Do not pass a
  subset while editing: cargo keys artifacts by feature set, so one `--features
  delta` or `--features iceberg` call compiles a second copy of the shared graph
  (deltalake, arrow, the AWS SDK) and everything downstream of it, and the next
  default-feature call compiles a third. Override `CARGO_FEATURES` only to
  reproduce a specific CI leg.
- `cargo check --all-features` is the edit loop: no codegen, no link, the same
  artifacts. Save `make check` for the end of a change, not after each edit.
- Integration tests are one target per crate: `tests/it.rs` lists every
  `tests/*.rs` file as a `#[path]` module. Add the line when you add a file;
  select one with `cargo test -p <crate> --test it -- <module>`.
- The edit loop is incremental and cheap: `incremental = true` on
  `[profile.dev]` (the `test` profile inherits it) makes a one-line edit to
  `pqbench` a 1-second `cargo check --all-features -p pqbench`. A build is slow
  only when it is not incremental — a new profile, a new feature set, a new
  target dir, or a fresh worktree — and a profile is part of every artifact's
  fingerprint, so changing one rebuilds all 236 crates of the graph.
- `CARGO_BUILD_JOBS` matters only for those full rebuilds; pass 6 for them
  (compilation leaves ten of twelve cores idle at 2 on this 14 GB machine).
  What spikes memory is the *link* phase of `cargo test`, which allocates
  gigabytes per binary; `split-debuginfo = "unpacked"` in `[profile.dev]` cuts
  each binary from ~490 MB to ~250 MB and the link cost with it.
- `sccache` is installed at `~/.local/bin/sccache`; `.cargo/config.toml` routes
  rustc through `.cargo/scripts/rustc-wrapper.sh`, which uses it when it is on
  `PATH`. Prepend `export PATH="$HOME/.local/bin:$PATH"` to cargo and make
  calls: it turns a repeat compile with the same flags into cache hits (8 s for
  a crate set into a fresh target dir). It does not help across a profile,
  feature-set or check/clippy change.
- Export one `CARGO_TARGET_DIR` shared by every worktree before building;
  never `cargo clean` it. A per-worktree target recompiles the ~70 AWS SDK and
  ~130 deltalake artifacts it already has. If a build says `Blocking waiting for
  file lock on build directory`, another session owns it: wait, do not take a
  private target dir.
- CI is two workflows. `.github/workflows/ci.yml` is the lightweight gate: fmt,
  aipnaming, `make check-docs`, isolation, and lfs. The heavy workflow,
  `.github/workflows/ci-heavy.yml`, runs one clippy over `--all-features` and a
  test matrix over default/aws/delta/iceberg/all-features (default and
  all-features also on arm64), plus the Python wheel and the
  lakehouse/Databricks jobs. Ignored e2e run via `TEST_FLAGS=--include-ignored`.
- `.cargo/config.toml` sets a portable codec baseline (no `-march=native`) so
  cached codec objects are valid on any machine. Do not make it native:
  objects tuned on one machine and restored from another's cache caused SIGILL.

## Repo layout

- `crates/pqbench/` — the library (bench, bytemass, codecs, compression,
  experiment, mass, profile, skill, lz, parquet, report, stats, viz)
- `crates/pqbench-cli/` — the `pqbench` command definitions and dispatch (a
  library), plus the thin binary
- `crates/docsgen/` — generates `docs/reference/cli.md` from the clap tree; a
  development tool, not a shipped binary
- `crates/aipnaming/`, `crates/aipnaming-cli/` — the AIP naming linter and its
  `aipnaming` binary
- `crates/docscheck/`, `crates/docscheck-cli/` — generate Rust tests from the
  runnable Markdown blocks; `docscheck sync` writes `tests/gen_*.rs`
- `crates/pqbench-cli/tests/gen_*.rs` — generated documentation tests (do not
  edit by hand; `make sync-docs`)
- `python/` — the wheel's README and build output; maturin packages the
  `pqbench` binary from `crates/pqbench-cli` (`bindings = "bin"`) as a
  data-only wheel. `make check-python` builds and smoke-tests it
- `skills/` — agent skills compiled into `pqbench skill`
- `scripts/` — build / sample / smoke-test helpers
- `docs/` — user documentation: `reference/cli.md` (generated from clap),
  `reference/`, `guides/`, `concepts/`
- `Makefile` + pre-commit hook — the gate

## References

- **Naming (AIP)** — Google API Improvement Proposals: https://google.aip.dev/
  (AIP-121/122 resource design, AIP-136 methods, AIP-140 fields, AIP-141 units,
  AIP-142 time, AIP-145 ranges, AIP-190 types, AIP-126 enums)
- **Tidyings** — Kent Beck, *Tidy First?* (O'Reilly, 2023): https://newsletter.kentbeck.com/
- **Unix philosophy** — Eric S. Raymond, *The Art of Unix Programming*
  (Basics of the Unix Philosophy): http://www.catb.org/~esr/writings/taoup/
  — course copy: https://cscie2x.dce.harvard.edu/hw/ch01s06.html
- **First principles thinking** — Jensen Huang (NVIDIA), Stanford GSB
  *View From The Top*: https://www.gsb.stanford.edu/insights/jensen-huang-how-use-first-principles-thinking-drive-decisions
