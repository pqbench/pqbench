# Visual demos

pqbench measures Parquet footers. A file is one input. A table is a snapshot
of files. The pipe is the same in every case:

```console run
$ pqbench table-file examples/quickstart.parquet | pqbench bytemass | pqbench format
column  codec         encodings                 bytes  values
------  ------------  ------------------------  -----  ------
id      UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    102       8
year    UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY     68       8
$ pqbench table-file examples/quickstart.parquet | pqbench bytemass | pqbench viz -o /tmp/report
```

Standard output is NDJSON (`Partition`, `TableFile`, `Blob`, `Bytemass`);
`pqbench format` renders a table. `pqbench viz` collects the bytemass stream into
a static HTML treemap.

The walkthroughs use the committed lakehouse fixtures under
[`docker/e2e-lakehouse/`](../docker/e2e-lakehouse) and the small Parquet
fixture in the test suite. Catalog pipes need `make lakehouse`.

## Selecting files with Unix tools

`bytemass` measures the files the `list files` stream names; which files is a
shell decision. The stream is one JSON record per line, so `jq` prunes file
records by `name`, and `sort`, `head`, or `awk` sample by name:

```console run delta json
# one file: keep only the part-* records
$ pqbench list partitions docker/e2e-lakehouse/table \
>   | pqbench list files \
>   | jq -c 'select(.name | contains("/files/part-"))' \
>   | pqbench bytemass \
>   | jq -c 'select(.name | endswith("/bytemass")) | {column: (.name | capture("/columns/(?<c>[^/]+)/bytemass$").c), compressed_bytes}' | tail -1
{"column":"label","compressed_bytes":72}

# first N by name: sort the file records, cap them, then measure
$ pqbench list partitions docker/e2e-lakehouse/table \
>   | pqbench list files \
>   | sort | head -10 \
>   | pqbench bytemass \
>   | jq -c 'select(.name | endswith("/bytemass")) | {column: (.name | capture("/columns/(?<c>[^/]+)/bytemass$").c), compressed_bytes}' | tail -1
{"column":"label","compressed_bytes":72}

# every Nth file
$ pqbench list partitions docker/e2e-lakehouse/table \
>   | pqbench list files \
>   | awk 'NR % 2 == 1' \
>   | pqbench bytemass \
>   | jq -c 'select(.name | endswith("/bytemass")) | {column: (.name | capture("/columns/(?<c>[^/]+)/bytemass$").c), compressed_bytes}' | tail -1
{"column":"label","compressed_bytes":72}
```

The `list files` stream carries each file's `credentials` on the record, so
the lease travels with it through `jq`, `sort`, and `head`: `bytemass` reads the
file its `blob` names.

## A table's partitions

`list partitions` groups a table's commits into epoch-aligned commit-time windows;
`list files` re-reads each window's commits and names the files they added.
The whole walk is three commands:

```console run delta
$ pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass | pqbench viz -o /tmp/report
```

![one table viz page](images/pqbench-lake-treemap.gif)

```console run delta
$ pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass | pqbench format
column  codec   encodings                 bytes  values
------  ------  ------------------------  -----  ------
id      SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
```

## Catalogs

The catalog comes from the environment (`PQB_ENDPOINT`, `PQB_TOKEN`). The
dialect is declared, not probed: `PQB_TABLE_FORMAT` is `unity` (the default) or
`iceberg`. The walk descends `list catalogs` → `list schemas`
→ `list tables`, checks and vends credentials, then enriches each table and lists
its files:

```console no-run
$ export PQB_ENDPOINT=… PQB_TOKEN=…
$ pqbench list tables CAT.SCHEMA |
    pqbench credentials check |
    pqbench credentials get |
    pqbench get table |
    pqbench list partitions |
    pqbench list files |
    pqbench bytemass | pqbench format
```

Every stage of a pipe writes NDJSON for the next stage; the last one prints the
table (`| pqbench format`). The local stand (`make lakehouse`) exports
`PQB_ENDPOINT` and `AWS_*` for the walk. See
[docker/e2e-lakehouse/README.md](../docker/e2e-lakehouse/README.md).

## One Parquet file

```console run
$ pqbench table-file crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet | pqbench bytemass | pqbench format
column            codec         encodings                  bytes  values
----------------  ------------  ------------------------  ------  ------
text              UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  743566    3000
label             UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    2469    3000
dataType          UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY     360    3000
communityName     UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY    2468    3000
datetime          UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY      87    3000
username_encoded  UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  446197    3000
url_encoded       UNCOMPRESSED  PLAIN,RLE,RLE_DICTIONARY  907117    3000
```

![pqbench CLI walkthrough](images/pqbench-bytemass.gif)

![byte-mass treemap](images/pqbench-bytemass.png)

## One Delta snapshot

`pqbench list partitions` needs `--features delta` to read a log. The stream is
NDJSON for `list files` and `bytemass`; `format` renders it:

```console run delta
$ pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass | pqbench format
column  codec   encodings                 bytes  values
------  ------  ------------------------  -----  ------
id      SNAPPY  PLAIN,RLE,RLE_DICTIONARY     66       3
label   SNAPPY  PLAIN,RLE,RLE_DICTIONARY     72       3
$ pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass | pqbench viz -o /tmp/report
```

![pqbench table CLI walkthrough](images/pqbench-delta-bytemass.gif)

![Delta byte-mass treemap](images/pqbench-delta-bytemass.png)

## One Iceberg snapshot

Iceberg needs `--features iceberg` (`iceberg-s3` for `s3://`). The committed
fixture stores data as `s3://lakehouse/...`, so measure it through the stand
(`make lakehouse`), not just the feature:

```console no-run
$ pqbench list partitions docker/e2e-lakehouse/iceberg > /tmp/iceberg.ndjson
```

With the stand up, walk the Iceberg REST catalog (`PQB_ENDPOINT=…
PQB_TABLE_FORMAT=iceberg`) and measure through it: `list tables | credentials
check | credentials get | get table | list partitions | list files | bytemass`.
`docs/demos/pqbench-iceberg-session.sh` runs the REST pipe when the stand answers
at `localhost:8181`.

## Regenerate the terminal recordings

Install `asciinema` and `agg`, then:

```sh
docs/demos/record.sh
```

That builds `pqbench` with `--features delta` (honoring `CARGO_TARGET_DIR`)
and records the Parquet and Delta sessions against committed fixtures.
The Iceberg REST pipe is not recorded here; it needs `make lakehouse`.

The table treemap GIF is one still of the viz page. Recapture it with
`docs/demos/capture-lake-treemap.sh` (Chrome + `ffmpeg`).
