# Guides

Task-oriented walkthroughs. Each one starts from a concrete situation — one
Parquet file, a Delta or Iceberg table, a Databricks workspace — and follows the
commands through to a result. They are the how, not the why; for the detail of
each command see the [Reference](../reference/cli.md).

- [Showcases](showcases.md) — real jobs on real data: triage which columns
  own the bytes, choose a codec, verify a layout change, and read SQuAD and a
  TPC-DS table from the wild.
- [Visual demos](demo.md) — the same measure-everything pipe over a Parquet
  file, a Delta table, an Iceberg table, and a catalog, with real output; ends
  with selecting files using `jq`, `sort`, `head`, and `awk`.
- [Delta tables](delta.md) — detect a log, list its active files, and feed
  them to `bytemass`.
- [Iceberg tables](iceberg.md) — load metadata and manifests, then run the
  same three-command walk.
- [Databricks authentication](auth.md) — create the service principal,
  grant metadata access, and mint the OAuth M2M bearer.
- [Performance](performance.md) — measure the metadata walk with `perf stat`
  and `perf record`, and read the result.
- [Python orchestration](python.md) — drive the CLI from Python with bash
  `-c`, a `Popen` chain, or plumbum, without giving up streaming.

New to pqbench? Start with [Getting started](../getting-started.md) instead: it
goes from install to a first treemap.
