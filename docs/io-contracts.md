# CLI input and output contracts

This is the reference for data crossing the `pqbench` CLI boundary. It describes
the behavior at `main` commit `84a66b3` and tracks later contract changes.
The [CLI guide](cli.md) explains how to use the commands; this page records what
their callers and downstream readers can exchange. [Issue #96](https://github.com/pqbench/pqbench/issues/96)
tracks the remaining enforcement work.

## Shared rules

- Unless a command says otherwise below, `--format auto` writes an aligned
  table to a terminal and newline-delimited JSON (NDJSON) to a pipe. `--format
  json` forces NDJSON; `--format table` forces the table. The table is for
  display, not for parsing.
- On commands with `-o FILE`, the file is an lz4-framed NDJSON stream regardless
  of stdout's format. Its contents are selected by the same command run. File
  extensions do not select the codec. A document path may contain ordinary
  JSON, NDJSON, or this lz4-framed stream; `-` reads standard input.
- Each JSON record has a `kind`. A family of streamed records normally starts
  with a `begin` record carrying `version: 1`, continues with data records,
  and ends with an `end` record. Most data and end records do **not** carry
  `version`; interpret them in the context of that family. Standalone
  resource and reference records carry their own version.
- The commands fail with a nonzero exit status and an `error:` message on
  standard error when an input cannot be read or classified. A producer can
  have written earlier NDJSON records before a later error. Consumers needing
  a complete stream should check the producer's exit status and its end
  record. For shell pipelines, use `set -o pipefail`.
- `env` fields may carry storage credentials. Treat a saved stream as secret
  material when it contains them. The legacy document reader and the metadata
  walk accept only `AWS_*` keys in document `env`.

## Commands

| Command | Accepted input | Machine output / side effect |
| --- | --- | --- |
| `lake` | Lake directory or URI; `pqbench.lake-source`, `pqbench.lake`, or v1 `pqbench.table-ref` document/stream | `pqbench.lake` begin/end around v1 `pqbench.table-ref` records |
| `table info` | v1 `pqbench.table-ref` stream | Enriched v1 `pqbench.table-ref` records |
| `table` | Delta/Iceberg path or URI; v1 `pqbench.table-ref`, `pqbench.lake`, `pqbench.remote-source`, or one-object `pqbench.table` document | `pqbench.table` begin/end, with `pqbench.table-log` and `pqbench.table-file` records |
| `bytemass` | Parquet paths/URIs or `pqbench.remote-source`, v1 table reference, one-object table document, or table stream | `pqbench.bytemass` begin/end, with `pqbench.bytemass-file`, `pqbench.bytemass-page`, and `pqbench.bytemass-row` records |
| `diff` | Two Parquet paths or complete bytemass streams; at most one `-` stdin operand | `pqbench.diff` begin/end and `pqbench.diff-column` records |
| `viz` | Bytemass stream from stdin or file | Writes `PREFIX.html`; terminal summary only |
| `dump` | Table path/URI or loaded table/lake document or stream | Writes Parquet files under the destination directory; summary on standard error |
| `metastore info` / `ls` | `pqbench.lake-source` first on stdin, or `PQB_*` environment | One `pqbench.metastore` record / `pqbench.catalog` records |
| `catalog info` / `ls` | Catalog name or `pqbench.catalog` refs on stdin, with lake-source context or `PQB_*` | `pqbench.catalog` / `pqbench.schema` records |
| `schema info` / `ls` | Schema name or `pqbench.schema` refs on stdin, with lake-source context or `PQB_*` | `pqbench.schema` / v2 `pqbench.table-ref` records |
| `tablev2 info` | Table name or v2 `pqbench.table-ref` refs on stdin, with lake-source context or `PQB_*` | Enriched v2 `pqbench.table-ref` records |
| `credentials check` / `get` | v2 `pqbench.table-ref` refs on stdin, with lake-source context or `PQB_*` | Eligible v2 refs unchanged / v2 refs with vended `env` |
| `ratelimit` | NDJSON records on stdin | The same records, delayed by `kind`; no document transformation |
| `lz` / `compression` | File path (`compression` expects suitable Parquet input) | `pqbench.lz` / `pqbench.compression` begin/end and corresponding row records; `compression` can also emit column records |
| `profile` | Parquet file path | `pqbench.profile` begin/end and `pqbench.profile-column` records |
| `experiment` | Parquet file path and optional rewrite specification | `pqbench.experiment` begin/end, trial and column records |
| `skill` | No name, or skill and optional document name | Listing: standalone v1 `pqbench.skill` JSON records. Named document: Markdown text |

The metadata walk (`metastore` through `credentials`) reads one NDJSON record
per line. Its optional `pqbench.lake-source` context must be first. Refs are
processed as they arrive, and commands with fan-out may emit results in
completion order rather than input order. `pqbench.table-ref` v1 belongs to
the `lake` / `table` path; v2 belongs to `schema ls` / `tablev2 info` /
`credentials`. The two versions are not interchangeable.

## Document families

| Family | Version and shape | Important fields / reader rule |
| --- | --- | --- |
| `pqbench.lake-source` | Standalone v1 document | `endpoint`, optional `token`, `catalog`, `schema`, `table_format`, `env`; supplies metadata-walk context or a lake listing |
| `pqbench.remote-source` | Standalone v1 document | Nonempty `inputs` array and optional `env` for `table` or `bytemass` |
| `pqbench.catalog`, `pqbench.schema` | Standalone v1 records | Name the next metadata level; `info` enriches the same kind |
| `pqbench.table-ref` | Standalone v1 or v2 record | v1: `id`, `uri`, optional `storage_path` and `env`; v2: catalog-qualified `id`, `uri`, and optional metadata / `env` |
| `pqbench.table` | v1 one-object document or v1 stream | Stream begins with table `id`, `format`, `uri`, and `snapshot_version`; log/file records use the `id` to belong to that table |
| `pqbench.lake` | v1 one-object document or v1 stream | Stream contains v1 table refs between begin/end |
| `pqbench.bytemass` | v1 stream | File, page, and row records appear between begin/end; `viz` consumes it, and `diff` requires a complete stream |
| `pqbench.diff`, `pqbench.lz`, `pqbench.compression`, `pqbench.profile`, `pqbench.experiment` | v1 output streams | The begin record declares the family version; sibling record kinds are listed in the command table above |
| `pqbench.skill` | Standalone v1 record | Name, description, and available document names |

The v1 table document reader checks the declared version on table, lake,
lake-source, remote-source, and table-ref documents. The metadata walk checks
the version on `pqbench.lake-source` and v2 table refs. Some consumers of
`pqbench.catalog` and `pqbench.schema` refs check `kind` without checking
`version`; most stream child records have no version to check. Deserializers
also generally accept extra fields. These are current acceptance behaviors,
not a promise that arbitrary new fields or versions are compatible.

## Contract changes

When a PR changes accepted inputs, JSON fields or record order, versioning,
stdout defaults, saved-file encoding, or exit behavior, update this reference
and add an entry here in the same PR. State the old and new behavior, affected
commands, compatibility or migration steps, and the PR link. A new document
version needs explicit producer and consumer behavior; changing a field while
keeping the same version still needs a change-log entry. Verify boundaries
through public CLI behavior and pipelines.

| Date | Change | Compatibility | Reference |
| --- | --- | --- | --- |
| 2026-10-06 | Baseline inventory of the current CLI contracts; no behavior change | Existing v1 streams and v1/v2 table-ref split | [`84a66b3`](https://github.com/pqbench/pqbench/commit/84a66b3), [#96](https://github.com/pqbench/pqbench/issues/96) |
