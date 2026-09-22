# Visual demos

pqbench measures Parquet footers. A file is one input. A table is a snapshot
of files. A lake is a list of tables. The pipe is the same in every case:

```sh
pqbench bytemass data.parquet
pqbench table ./delta-table | pqbench bytemass
pqbench table ./iceberg-table | pqbench bytemass
pqbench lake ./warehouse | pqbench table | pqbench bytemass --json
```

A TTY prints a short summary and requires `-o` (zstd NDJSON). A pipe streams
NDJSON (`pqbench.table-ref`, `pqbench.table` begin/file/end, `pqbench.bytemass-row`).
`--d3` is one table at a time.

The walkthroughs use the committed lakehouse fixtures under
[`docker/e2e-lakehouse/`](../docker/e2e-lakehouse) and the small Parquet
fixture in the test suite. Catalog pipes need `make lakehouse`.

## Selecting files with Unix tools

`bytemass` measures the files the `table` stream names; which files is a shell
decision. The stream is one JSON record per line, so `jq` prunes file records
by `path`, and `sort`, `head`, or `awk` sample by name:

```sh
# one partition: keep the begin/end records, drop the other files
pqbench table ./delta-table \
  | jq -c 'select(.kind != "pqbench.table-file" or (.path | startswith("year=2024/")))' \
  | pqbench bytemass --json

# first N by path: sort the file URIs, cap them, then measure
pqbench table ./delta-table \
  | jq -r 'select(.kind == "pqbench.table-file") | .uri' \
  | sort | head -10 \
  | xargs pqbench bytemass --json

# every Nth file
pqbench table ./delta-table \
  | jq -r 'select(.kind == "pqbench.table-file") | .uri' \
  | awk 'NR % 2 == 1' \
  | xargs pqbench bytemass --json
```

The first form keeps the per-table `env` (S3/Unity credentials) on the begin
record, so it works for remote tables. Dropping to bare `uri`s (`jq -r`) is
local-only: pass credentials in the environment when you use `xargs`.

## A lake of tables

A directory, `file://` URI, or `s3://` prefix is walked until a table marker
that `pqbench table` also accepts. Children of a table are not searched.

```sh
pqbench lake docker/e2e-lakehouse -o lake.ndjson.zst
pqbench lake docs/demos/lake.json | pqbench table | pqbench bytemass --json
```

![pqbench lake CLI walkthrough](images/pqbench-lake.gif)

[docs/demos/lake.json](demos/lake.json) names the Unity Delta fixture so the
full `lake | table | bytemass` pipe works without rustfs. The Iceberg fixture
in the same tree lists; measuring it needs the stand (`iceberg-s3`).

`--json` is the same NDJSON stream. For a treemap, load one table and pass
`--d3`. The image is one still of that page, not a lake click-through:

![one table --d3 page](images/pqbench-lake-treemap.gif)

```sh
pqbench table docker/e2e-lakehouse/table | pqbench bytemass --d3 > treemap.html
```

## Catalogs

A `pqbench.lake-source` lists a catalog. `GET /v1/config` with a `defaults`
object is Iceberg REST; a 200 without `defaults`, or HTTP 404, is Unity.

```sh
pqbench lake docs/demos/unity.json | pqbench table | pqbench bytemass
pqbench lake docs/demos/iceberg-rest.json | pqbench table | pqbench bytemass
```

Those documents point at the local stand. See
[docker/e2e-lakehouse/README.md](../docker/e2e-lakehouse/README.md).

## One Parquet file

```sh
pqbench bytemass crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet -o bytemass.ndjson.zst
pqbench bytemass crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet --json
pqbench bytemass crates/pqbench-cli/tests/fixtures/small_reddit_none.parquet --d3 > treemap.html
```

![pqbench CLI walkthrough](images/pqbench-bytemass.gif)

![byte-mass treemap](images/pqbench-bytemass.png)

## One Delta snapshot

`pqbench table` needs `--features delta` to load a log. A TTY prints a
summary and requires `-o`; a pipe streams NDJSON for `bytemass`:

```sh
pqbench table docker/e2e-lakehouse/table -o table.ndjson.zst
pqbench table docker/e2e-lakehouse/table | pqbench bytemass
pqbench table docker/e2e-lakehouse/table | pqbench dump --row-groups first:1 --output sample.parquet
pqbench table docker/e2e-lakehouse/table | pqbench dump --sample first:1 --csv
pqbench table docker/e2e-lakehouse/table | pqbench bytemass --d3 > treemap.html
```

![pqbench table CLI walkthrough](images/pqbench-delta-bytemass.gif)

![Delta byte-mass treemap](images/pqbench-delta-bytemass.png)

## One Iceberg snapshot

Iceberg needs `--features iceberg` (`iceberg-s3` for `s3://`). The committed
fixture stores data as `s3://lakehouse/...`, so measure it through the stand:

```sh
pqbench lake docker/e2e-lakehouse/iceberg -o iceberg.ndjson.zst
pqbench lake docs/demos/iceberg-rest.json | pqbench table | pqbench bytemass
```

`docs/demos/pqbench-iceberg-session.sh` lists the fixture; set
`PQBENCH_LAKEHOUSE=1` when the stand is up to run the REST pipe.

## Regenerate the terminal recordings

Install `asciinema` and `agg`, then:

```sh
docs/demos/record.sh
```

That builds `pqbench` with `--features delta` (honoring `CARGO_TARGET_DIR`)
and records the lake, Parquet, and Delta sessions against committed fixtures.
The Iceberg REST pipe is not recorded here; it needs `make lakehouse`.

The table treemap GIF is one `--d3` still. Recapture it with
`docs/demos/capture-lake-treemap.sh` (Chrome + `ffmpeg`).
