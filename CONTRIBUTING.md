# Contributing to pqbench

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
make check        # = fmt-check + clippy -D warnings + test
```

- `make check` is the gate and runs automatically in the pre-commit hook.
- e2e (single-threaded only):
  ```
  cargo test -p pqbench-cli --test bytemass_e2e -- --test-threads=1
  ```
- `make samples` fetches a few open parquet datasets into `local/samples/` for
  manual testing.
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
- Fields are **nouns, not verbs**; booleans use `is_`/`has_`/`can_`; keep a
  **single root** for a family (`compress`/`decompress`, not a mix of verb and
  noun forms).
- Units spelled out, never ambiguous abbreviations: `megabytes_per_second`, not
  `mbps`; `_estimate` for a measurement; `_duration`/`_durations` for spans.
- Ranges use `first`/`last`; no bare `time`.
- Enums: prefer `UpperCamelCase` variants (idiomatic Rust), even though AIP-126
  says `UPPER_SNAKE_CASE` — that rule is directional.

## 5. Update AGENTS.md

`AGENTS.md` is the source of truth for agents. If you change the gate, the loop,
or a convention, update `AGENTS.md` so the next agent follows it. Also record
every file you edit in your reply as a plain path reference (see AGENTS.md
*Reporting edits*).

## 6. Commit

- **Commit only when the user asks.**
- Each tidy is its own commit; keep behavior changes separate.
- The pre-commit hook runs `make check`, so the gate must be green.

## 7. Contributions & licensing

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
