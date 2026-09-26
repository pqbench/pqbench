# pqbench documentation

pqbench measures how Parquet files spend bytes (`bytemass`, `profile`) and how
well codecs compress them (`lz`, `compression`, `experiment`). Delta, Iceberg,
and catalog support are feature-gated. Commands compose on pipes: each writes a
versioned JSON document the next command reads.

New here? **[Get started](getting-started.md)** walks one file to a byte-mass
treemap. Every command is in the **[CLI reference](cli.md)**.

The canonical pipe — list a lake, load each table's snapshot, measure its
files, draw the result:

```sh
pqbench lake ./warehouse | pqbench table | pqbench bytemass | pqbench viz -o report
```

A terminal prints an aligned table; a pipe streams NDJSON. `--format table|json`
overrides either, and `-o` additionally writes the zstd NDJSON stream.

## The docs, by kind

A tutorial to learn from, how-to guides for a task, and reference for the
details.

### Tutorial

- [Getting started](getting-started.md) — install, first measurement, the lake
  pipeline, a treemap. Start here.

### How-to guides

- [Visual demos](demo.md) — Parquet, Delta, Iceberg, and lake pipes with real
  output, and how to select files with Unix tools.
- [Delta tables](delta.md) — detect a log, load a snapshot, `table | bytemass`,
  backends, limitations.
- [Iceberg tables](iceberg.md) — load metadata and manifests, `table |
  bytemass`, limitations.
- [Docker](docker.md) — build, run, publish, and benchmark in a container.

### Reference

- [CLI reference](cli.md) — the task-oriented command table, the document
  kinds, repeated flags, and auth for `s3://` and catalogs.
- [`profile`](profile.md) — per-column facts from a decoded row sample.
- [`experiment`](experiment.md) — rewrite a sample and measure the result.
- [`viz`](viz.md) — collect a bytemass stream into a static HTML treemap.
- [Agent skills](skill.md) — the skills compiled into `pqbench skill`.
- [Python bindings](../python/README.md) — install the wheel and call every
  command in-process.

## Repository documents

- [`README.md`](../README.md) — the landing page and quick start.
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — the change loop, the gate, naming.
- [`AGENTS.md`](../AGENTS.md) — the working rules for agents.
