# Getting started

pqbench measures **how a Parquet file spends bytes** and how well codecs
compress it. It reads the footer, not the pages, so it is fast on files of any
size. Every command writes a typed `pqbench.v1` document, so commands compose on
pipes: `list files` names a table's active files, `bytemass` measures them,
`summarize columns` folds the measurements into one estimate per table column,
and `format --html` draws the result.

This page is a guided first run. It uses the committed
[`examples/quickstart.parquet`](../examples/quickstart.parquet) smoke sample and
the lakehouse fixtures under `docker/e2e-lakehouse/`. For an exhaustive command
reference, read [CLI reference](reference/cli.md).

## Install

Install the released `pqbench` binary (Linux and macOS, x86_64 and arm64):

```sh
curl -LsSf https://github.com/pqbench/pqbench/releases/latest/download/pqbench-cli-installer.sh | sh
```

Supported targets and the release process are in
[Distribution](reference/distribution.md).

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
$ pqb table-file examples/quickstart.parquet | pqb bytemass | pqb fmt
column  type   codec         encoding  bytes  dict bytes  bytes/row  values
------  -----  ------------  --------  -----  ----------  ---------  ------
id      INT64  UNCOMPRESSED  dict        102          80      12.75       8
year    INT32  UNCOMPRESSED  dict         68          46       8.50       8
```

`profile` decodes a row sample and reports per-column facts (nulls, distinct
values, entropy, runs). It reads values, so it is the complement of `bytemass`,
which reads only the footer.

```console run
$ pqb profile examples/quickstart.parquet | pqb fmt
id                           column  kind     values  nulls  ndv
---------------------------  ------  -------  ------  -----  ---
examples/quickstart.parquet  id      integer       8      0    8
examples/quickstart.parquet  year    integer       8      0    8
```

## The metadata walk

A *table* is one snapshot's log and active files. The walk lists a table's
active files, then measures them:

```console run delta
$ cat docker/e2e-lakehouse/table.ndjson | pqb ls files | pqb bytemass | pqb fmt
column  type        codec   encoding  bytes  dict bytes  bytes/row  values
------  ----------  ------  --------  -----  ----------  ---------  ------
id      INT64       SNAPPY  dict         66          37      22.00       3
label   BYTE_ARRAY  SNAPPY  dict         72          43      24.00       3
```

Reading a Delta or Iceberg log needs its cargo feature (`delta` / `iceberg`;
`delta-s3` / `iceberg-s3` for `s3://`). A missing feature fails at runtime and
names itself. See [Delta tables](guides/delta.md) and [Iceberg tables](guides/iceberg.md).

## Draw the result

`summarize columns` folds the bytemass stream into one estimate per table
column, and `format --html` draws those rows as a static HTML treemap. No server, no
build step:

```console run
$ pqb table-file examples/quickstart.parquet | pqb list columns | pqb summarize columns | pqb format --html > /tmp/report.html
```

The page lands at `/tmp/report.html`; open it in a browser. See [format](reference/format.md).

## Where to go next

- **Commands** — the [CLI reference](reference/cli.md) documents every command;
  [Documents](reference/documents.md) and [Authentication](reference/authentication.md)
  are separate pages.
- **Real jobs on real data** — [Showcases](guides/showcases.md) triages a file's
  bytes, picks a codec, and verifies a layout change, on public datasets.
- **More walkthroughs** — [Visual demos](guides/demo.md) has Parquet, Delta, Iceberg, and
  catalog pipes with real output.
- **Tune a layout** — [profile](reference/cli.md#pqbench-profile) reports what is in a sample;
  [format --html](reference/format.md) shows where an existing table's bytes go.
