# aipnaming

Lint Rust names against the AIP naming conventions the workspace follows
(see [`AGENTS.md`](../../AGENTS.md) *Naming guidelines (AIP)*). It is a
textual linter: a tree-sitter front end (`src/rust.rs`) fills a
backend-neutral declaration model (`src/decl.rs`), and the rules check that
model. Rule ids mirror Google's `api-linter` — `<aip-proposal>/<rule>` — so a
finding maps back to a published rule page.

The workspace's naming conventions differ from AIP in two deliberate ways, and
the rules reflect them:

- Enum variants stay `UpperCamelCase` (idiomatic Rust), so AIP-126 casing is
  available but not on by default.
- Types come from the source as written, not a type checker, so rules that
  need a full type (a `bool` behind an alias, a repeated field) are advisory.

## Rules

| Rule | Checks |
| --- | --- |
| `aip-190/casing` | Types `UpperCamelCase`, functions/fields `snake_case`, enum variants `UpperCamelCase` |
| `aip-140/underscores` | No leading/trailing or doubled underscores |
| `aip-140/abbreviations` | `config` not `cfg`/`conf`, `id` not `identifier`, … |
| `aip-140/prepositions` | Fields name a thing, not a relation (`timeout`, not `timeout_for`) |
| `aip-136/method-prepositions` | Method names avoid a trailing preposition |
| `aip-140/verbs` | Fields are nouns, not imperative verbs |
| `aip-140/booleans` | Booleans omit the verb prefix (`dictionary`, not `is_dictionary`) |
| `aip-140/reserved-words` | Identifiers avoid language keywords and reserved words |
| `aip-136/async-name` | Long-running methods take the AIP-136 name |
| `aip-140/plural` | A sequence field is plural, a scalar field singular |
| `aip-141/units` | Units spelled out (`megabytes_per_second`, not `mbps`) |
| `aip-190/american-english` | American spelling (`color`, not `colour`) |
| `aip-142/time-field-names` | `_time` for instants, `_duration`/`_durations` for spans |
| `aip-142/duration-offset-comment` | A duration/offset documents its unit |
| `aip-145/ranges` | Ranges use `first`/`last`, not `min`/`max` |

## Running it

```sh
cargo run -p aipnaming-cli -- crates/                 # lint the workspace
cargo run -p aipnaming-cli -- --statistics crates/    # add a per-rule count
cargo run -p aipnaming-cli -- --list-rules            # the rule ids
cargo run -p aipnaming-cli -- --rule aip-140/booleans crates/
cargo run -p aipnaming-cli -- --output-format github crates/   # PR annotations
```

Findings print as `file:line:column: severity[rule]: message`. Exit status is 0
when clean, 1 when findings exist, and 2 for usage or io errors. The
`aipnaming` job in `.github/workflows/ci.yml` runs it over `crates/`; the local
`make check` runs the crate's tests but not the linter, so run it before
pushing.

## Exceptions

A finding can be waived in place with a comment on the finding's line (or the
line above), naming the rule or `all`:

```rust
// aipnaming: allow(aip-145/ranges)
pub min_value: Option<String>,
```

Use it for a deliberate exception, the way the rules' own tests do.

## Word data

The lexicon is data, not code, and lives in two plain-text files under
`src/data/` (loaded by `src/data/mod.rs`):

- `words.txt` — the lexicon the part-of-speech tagger (`src/words.rs`)
  consults.
- `rules.txt` — the smaller vocabularies the individual checks use.

A file is a sequence of `# set:<name>` (one token per line) or
`# pairs:<name>` (two whitespace-separated columns) sections; `//` and blank
lines are ignored. Edit these files to change a vocabulary, not the checks.
