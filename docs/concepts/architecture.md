# Architecture

The rules that keep pqbench's commands decoupled, third-party code isolated,
and the dependency tree small. [`AGENTS.md`](../../AGENTS.md) covers naming and
tidyings; this page covers structure.

## Layers

- `crates/pqbench` — the library: command implementations, domain types, and
  the third-party adapters.
- `crates/pqbench-cli` — a thin wrapper: argument parsing, the JSON documents,
  and output formatting. No command logic.

## Commands

The walk's resource commands (`metastore`, `catalog`, `schema`, `table`,
`file`, `credentials`) are folders — one per resource, with a subcommand folder
per verb. The measurement commands (`bytemass`, `compression`, `lz`) keep an
`api.rs` and split the implementation across named modules; `profile`, `diff`,
`skill`, and `ratelimit` are single modules, and `summarize` and `viz` are small
folders. The shared support (`report`, `mass`, `stats`, `time`, `config`,
`dialect`, `http`, `capability`, `codecs`) sits at the crate root.

```text
crates/pqbench/src/
  <resource>/
    <verb>/{api.rs, impl.rs, mod.rs}
  bytemass/{api.rs, raw.rs, analytics.rs, text.rs, json.rs, …}
  third_party/
    <crate>/{api.rs, impl.rs, mod.rs}
    <crate>/impl/<module>.rs
```

- `api.rs` — the command's public entry point and the few helpers a caller
  needs. It is the whole public surface.
- `impl.rs` — the internals, private and free to change.

Rules:

- Every command stands on its own and is architected from above: its design
  starts at the public surface and shapes the implementation beneath it, never
  by lifting structure out of another command.
- A command is self-contained. It never imports another command and shares no
  implementation with one; repetition between commands is the accepted price of
  decoupling.
- A command folder never grows a utility module for other commands.
- Command code is always compiled. A missing capability is a runtime error, not
  a reason to remove code, so command code has no dead-code allowances.

## Resource hierarchy

Commands mirror the resource hierarchy of the data model — metastore, catalog,
schema, table, file. Each level's `list` module lists that level's resources
(the parent rides the argument, or parent refs on standard input); `get` reads
one resource.

## Interfaces

- The public surface is the command's entry point, and the less surface the
  better.
- Inputs are plain values the caller has. Configuration, pagination, and
  dialect state belong to the implementation, never to the signature.
- No abstractions in the public surface: no traits, no generic helpers, no
  shared protocol layer. Simple and readable beats clever and reusable.
- Where two implementations serve the same operation they keep the same shape,
  so dispatch stays flat.

## third_party

- One folder per third-party crate: `api.rs` is the isolated surface; `impl.rs`
  and its `impl/` submodules are the only code that names the crate. Feature
  flags live only under `impl`, enforced by `scripts/check_isolation.sh`.
- An adapter wraps a third-party crate and nothing else: no domain types, no
  other modules of the crate.
- Protocol code we speak ourselves is not third-party code, even when it runs
  over a third-party transport.
- The adapter mirrors only the minimal part of the dependency's interface that
  is needed.

## Features

- A command never adds a feature. Features map to third-party dependencies; a
  command reuses the feature that gates the dependency it needs.
- Code that does not name a third-party crate never sees a feature flag.
- CI builds one test leg per feature, and none for features with no distinct
  compilation behavior.

## Serialization

- The documents commands exchange are the CLI's communication language: the
  resource types live in `pqbench::v1` (the profile record is
  `pqbench::profile::ColumnProfile`), and the CLI renders them.
- Library commands take and return those resources; presentation stays in the
  CLI.
- The CLI parses an input document, calls one command function, and formats the
  result.

## Why

- Self-contained commands change, test, and ship independently.
- Duplication over coupling: a copied loop is cheaper than a shared module two
  commands must change together.
- A minimal public surface keeps signatures readable and internals free.
- One feature per dependency keeps the default build and the CI matrix small.
- Serialization in the CLI keeps the library independent of a presentation
  format.
