# Getting started

pqbench measures **how a Parquet file spends bytes** and how well codecs
compress it. It reads the footer, not the pages, so it is fast on files of any
size. Every command writes a versioned JSON document, so commands compose on
pipes: `table ls` lists a table's partitions, `partition ls` their files,
`bytemass` measures them, `viz` draws the result.

This page is a guided first run. It uses the committed
[`examples/quickstart.parquet`](../examples/quickstart.parquet) smoke sample and
the lakehouse fixtures under `docker/e2e-lakehouse/`. For an exhaustive command
reference, read [cli.md](cli.md).

## Install

Docker is the fastest way to try it — no Rust toolchain, no build:

```sh
docker run --rm -v "$PWD:/data:ro" pqbench/pqbench:latest bytemass /data/your.parquet
```

The published image is a portable baseline build; see [docker.md](docker.md)
for how its numbers compare to a native build. To build from source instead:

```sh
git clone https://github.com/pqbench/pqbench.git
cd pqbench
make build          # or: cargo build --release
```

## Measure one file

`bytemass` reports the on-disk bytes each column takes, per row. On a terminal
an aligned table; on a pipe, NDJSON.

```console run
$ pqbench bytemass examples/quickstart.parquet --format table
column  type   codec         encodings                 bytes  values
------  -----  ------------  ------------------------  -----  ------
id      INT64  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    102       8
year    INT32  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY     68       8
files: 1
rows: 8
columns: 2
```

`profile` decodes a row sample and reports per-column facts (nulls, distinct
values, entropy, runs). It reads values, so it is the complement of `bytemass`,
which reads only the footer.

```console run
$ pqbench profile examples/quickstart.parquet --format table
id                           column  kind     values  nulls  ndv
---------------------------  ------  -------  ------  -----  ---
examples/quickstart.parquet  id      integer       8      0    8
examples/quickstart.parquet  year    integer       8      0    8
files: 1
rows: 8
columns: 2
```

## The metadata walk

A *table* is one snapshot's log and active files; a *partition* is a lens on
those files. The walk lists a table's partitions, then their files, then
measures them:

```console run delta
$ pqbench table ls docker/e2e-lakehouse/table | pqbench partition ls | pqbench bytemass --format table
column  type        codec   encodings                 bytes  values
------  ----------  ------  ------------------------  -----  ------
id      INT64       SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   BYTE_ARRAY  SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
files: 1
rows: 3
columns: 2
```

Reading a Delta or Iceberg log needs its cargo feature (`delta` / `iceberg`;
`delta-s3` / `iceberg-s3` for `s3://`). A missing feature fails at runtime and
names itself. See [delta.md](delta.md) and [iceberg.md](iceberg.md).

## Draw the result

`viz` collects a bytemass stream into a static HTML treemap. No server, no
build step:

```console run
$ pqbench bytemass examples/quickstart.parquet | pqbench viz -o /tmp/report
```

The page lands at `/tmp/report.html`; open it in a browser. See [viz.md](viz.md).

## Where to go next

- **Commands** — [cli.md](cli.md) has the task-oriented table, documents, and
  auth for `s3://` and catalogs.
- **More walkthroughs** — [demo.md](demo.md) has Parquet, Delta, Iceberg, and
  catalog pipes with real output.
- **Tune a layout** — [experiment.md](experiment.md) rewrites a sample and
  measures it; [profile.md](profile.md) reports what is in it.
