# AGENTS.md

Working rules for agents in the pqbench repository.

## Principles

- **No ad-hoc workarounds.** If the environment is unsuitable, fix the
  environment itself; avoid PATH hacks, musl/custom-linker overrides, bundled
  broken artifacts, or per-machine config. No hardcoded paths in scripts.
- **Fast compilation time.** Compile time matters. Keep the dependency tree
  small; prefer the default (gnu) target; reuse cached artifacts (shared
  `CARGO_TARGET_DIR`) and incremental builds; avoid heavy new dependencies
  (e.g. anything that drags in a full LLVM rebuild).
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
- Gate (pre-commit): `make check` = `fmt-check` + `clippy -D warnings` + `test`
- Features: pass `CARGO_FEATURES` to any target (quote values with a space),
  e.g. `make check CARGO_FEATURES=--all-features`,
  `make test CARGO_FEATURES="--features aws"`.
- CI (`.github/workflows/ci.yml`) runs fmt, one clippy over `--all-features`,
  and a test matrix over default/aws/delta/all-features (default and
  all-features also on arm64). Ignored e2e run via `TEST_FLAGS=--include-ignored`.
- CI overrides `CXXFLAGS` with a portable baseline (no `-march=native`) so cached
  codec objects are valid on any runner. Do not remove it: native-tuned objects
  restored from another runner's cache caused SIGILL.

## Visual demos and public samples

- Regenerate the terminal recordings with `docs/demos/record.sh`. It needs
  Docker, `asciinema`, and `agg`; it builds `pqbench-cli` with the `delta`
  feature in persistent, architecture-specific Cargo volumes, then records the
  sessions in `docs/demos/` and renders `docs/images/*.gif`.
- The D3 treemap PNGs in `docs/images/` are browser screenshots of `--d3`
  output, not script-generated; recapture them by hand when the pages change.
- Stage external inputs once under the ignored `.docker-data/` directory. The
  source mount is read-only, so redirect generated output to `.docker-data/`
  (or elsewhere on the host) rather than into the checkout, and reuse the
  dataset paths documented in [docs/demo.md](docs/demo.md).
- `make samples` fetches the small Apache/Hugging Face fixtures used by tests;
  the demo datasets below are larger and live only under `.docker-data/`.

The following anonymous endpoints were verified on 2026-09-19. Prefer these
reusable inputs for demos instead of generating large fixtures from scratch.

- Tiny Parquet smoke test (public GCS, 1,849 bytes):
  `https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.parquet`
- Official Apache Parquet compatibility fixture (HTTPS, 1,698 bytes):
  `https://raw.githubusercontent.com/apache/parquet-testing/master/data/alltypes_dictionary.parquet`
  Other format and edge-case fixtures live in
  `https://github.com/apache/parquet-testing/tree/master/data`.
- Medium Parquet demo (public Azure Blob, 7,218,937 bytes):
  `https://azureopendatastorage.blob.core.windows.net/censusdatacontainer/release/us_population_county/year=2000/part-00177-tid-926394737839939592-51ecde30-440a-40fd-9b41-831814678ab5-1919150.c000.snappy.parquet`
- Realistic Parquet visualization demo (NYC yellow taxi, HTTPS, 49,961,641
  bytes):
  `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`
- Small anonymous Delta table with three language partitions (public S3,
  roughly 50 KB):
  `s3://daft-public-datasets/red-pajamas/stackexchange-sample-north-germanic-deltalake`
  HTTPS root:
  `https://daft-public-datasets.s3.us-west-2.amazonaws.com/red-pajamas/stackexchange-sample-north-germanic-deltalake/`
  First log:
  `https://daft-public-datasets.s3.us-west-2.amazonaws.com/red-pajamas/stackexchange-sample-north-germanic-deltalake/_delta_log/00000000000000000000.json`

Use the GCS file for a fast happy-path check, Apache fixtures for compatibility
and regression coverage, Azure Census for a medium cloud example, NYC Taxi for
a meaningful treemap demo, and the Daft S3 table for `pqbench delta`.

Unity Catalog sample tables such as `samples.nyctaxi`, `samples.tpcds_sf1`, and
`samples.tpch` require a Databricks workspace and are not anonymous object URLs.
Do not use the formerly public `pandemicdatalake` Bing COVID Parquet URL or
`ursa-labs-taxi-data` S3 bucket in unattended tests; live checks returned
access errors on 2026-09-19.

## Repo layout

- `crates/pqbench/` — the library (bench, bytemass, codecs, compression, lz,
  parquet, report, stats)
- `crates/pqbench-cli/` — the `pqbench` binary (thin wrapper over the library)
- `scripts/` — build / sample / smoke-test helpers
- `docs/` — user documentation (e.g. `docker.md`)
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
