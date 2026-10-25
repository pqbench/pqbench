# Getting started

pqbench measures **how a Parquet file spends bytes** and how well codecs
compress it. It reads the footer, not the pages, so it is fast on files of any
size. Every command writes a versioned JSON document, so commands compose on
pipes: `list partitions` lists a table's partitions, `list files` their files,
`bytemass` measures them, `viz` draws the result.

This page is a guided first run. It uses the committed
[`examples/quickstart.parquet`](../examples/quickstart.parquet) smoke sample and
the lakehouse fixtures under `docker/e2e-lakehouse/`. For an exhaustive command
reference, read [cli.md](cli.md).

## Install

Install the released `pqbench` binary (Linux and macOS, x86_64 and arm64):

```sh
curl -LsSf https://github.com/pqbench/pqbench/releases/latest/download/pqbench-cli-installer.sh | sh
```

Supported targets and the release process are in
[Distribution](distribution.md).

To build from source instead:

```sh
git clone https://github.com/pqbench/pqbench.git
cd pqbench
make build          # or: cargo build --release
```

## Measure one file

`bytemass` reports the on-disk bytes each column takes. On a terminal
an aligned table; on a pipe, NDJSON.

```console run
$ pqbench table-file examples/quickstart.parquet | pqbench bytemass | pqbench format
column  codec         encodings                 bytes  values
------  ------------  ------------------------  -----  ------
id      UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    102       8
year    UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY     68       8
```

`profile` decodes a row sample and reports per-column facts (nulls, distinct
values, entropy, runs). It reads values, so it is the complement of `bytemass`,
which reads only the footer.

```console run
$ pqbench profile examples/quickstart.parquet | pqbench format
id                           column  kind     values  nulls  ndv
---------------------------  ------  -------  ------  -----  ---
examples/quickstart.parquet  id      integer       8      0    8
examples/quickstart.parquet  year    integer       8      0    8
```

## The metadata walk

A *table* is one snapshot's log and active files; a *partition* is a lens on
those files. The walk lists a table's partitions, then their files, then
measures them:

```console run delta
$ cat docker/e2e-lakehouse/table.ndjson | pqbench list partitions | pqbench list files | pqbench bytemass | pqbench format
column  codec   encodings                 bytes  values
------  ------  ------------------------  -----  ------
id      SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
```

Reading a Delta or Iceberg log needs its cargo feature (`delta` / `iceberg`;
`delta-s3` / `iceberg-s3` for `s3://`). A missing feature fails at runtime and
names itself. See [delta.md](delta.md) and [iceberg.md](iceberg.md).

## Draw the result

`viz` collects a bytemass stream into a static HTML treemap. No server, no
build step:

```console run
$ pqbench table-file examples/quickstart.parquet | pqbench bytemass | pqbench viz -o /tmp/report
```

The page lands at `/tmp/report.html`; open it in a browser. See [viz.md](viz.md).

## Where to go next

- **Commands** — [cli.md](cli.md) has the task-oriented table, documents, and
  auth for `s3://` and catalogs.
- **Real jobs on real data** — [showcases.md](showcases.md) triages a file's
  bytes, picks a codec, and verifies a layout change, on public datasets.
- **More walkthroughs** — [demo.md](demo.md) has Parquet, Delta, Iceberg, and
  catalog pipes with real output.
- **Tune a layout** — [profile.md](profile.md) reports what is in a sample;
  [viz.md](viz.md) shows where an existing table's bytes go.
