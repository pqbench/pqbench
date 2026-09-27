# pqbench documentation

pqbench measures how Parquet files spend bytes (`bytemass`, `profile`) and how
well codecs compress them (`lz`, `compression`, `experiment`). Delta, Iceberg,
and catalog support are feature-gated. Commands compose on pipes: each writes a
versioned JSON document the next command reads.

New here? **[Get started](getting-started.md)** walks one file to a byte-mass
treemap. Every command is in the **[CLI reference](cli.md)**.

The canonical pipe — list a lake, load each table's snapshot, measure its
files, draw the result:

```console run delta
$ pqbench lake docker/e2e-lakehouse --include table | pqbench table | pqbench bytemass --format table
column  type        codec   encodings                 bytes  values
------  ----------  ------  ------------------------  -----  ------
id      INT64       SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   BYTE_ARRAY  SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
files: 1
rows: 3
columns: 2
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
- [Databricks auth](auth.md) — create the service principal, grant metadata
  access, mint the OAuth M2M bearer.
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
- [Architecture](architecture.md) — the command layout, third-party isolation,
  features, and the wire format.
