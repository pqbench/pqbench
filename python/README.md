# Python

The `pqbench` wheel ships the same binary as the release archives.
`pip install pqbench` puts `pqbench` on your `PATH`; there is no Python
extension and no importable module. The CLI is the interface, and the files it
writes are the data.

## Install

```console no-run
$ pip install pqbench
```

The wheel carries every storage backend (`delta-s3`, `iceberg-s3`), so `s3://`
reads and Delta/Iceberg tables work out of the box. It is a data-only wheel:
the binary lives in the wheel's `.data/scripts/` directory, which pip installs
into the environment's `bin/`.

From a checkout:

```console no-run
$ python -m pip install maturin
$ maturin build --release --out python/dist
$ python -m pip install python/dist/*.whl
```

## Use from Python

Python runs the CLI and reads what it wrote. Nothing else.

```python
import os
import subprocess

def run(script: str) -> None:
    subprocess.run(
        ["bash", "-o", "pipefail", "-c", script],
        check=True,
        env=dict(os.environ),   # PQB_ENDPOINT / PQB_TOKEN live here, not argv
    )

run("""
    set -euo pipefail
    pqbench list catalogs \
      | pqbench ratelimit --rate 15 \
      | pqbench list schemas \
      | pqbench list tables \
      | pqbench credentials check \
      | pqbench credentials get \
      | pqbench get table \
      | pqbench list files \
      | zstd -3 -T1 > .files.jsonl.zst
    mv .files.jsonl.zst files.jsonl.zst
""")
```

`pipefail` is what makes the pipeline fail when an earlier stage does: without
it, `zstd` sees EOF from a dead producer, compresses an empty or truncated
stream, and exits 0. The hidden temp name plus `mv` keeps a reader from seeing a
partial part; it is required only when something reads the directory while the
pipeline runs.

Then read the artifact with whatever engine you already use:

```python
import polars as pl

files = pl.scan_ndjson("files.jsonl.zst")
```

```python
files = spark.read.json("files.jsonl.zst")
```

```python
import duckdb

duckdb.sql("select * from read_json_auto('files.jsonl.zst')")
```

## What the wheel is not

- There is no `pqbench.native`, no `walk`, and no `DataFrame -> DataFrame` API.
  Those would add a second concurrency model (Python's) beside the one the
  binary already has, and a surface to keep in sync.
- `python -m pqbench` is not provided; run `pqbench`.
- The binary is the only artifact. Composition is the shell's, and the reader is
  the user's.

If you need the library in-process — a Spark `mapInPandas` UDF that measures
footers per batch, say — use the Rust crate (`crates/pqbench`) or call the
binary. A narrow binding can be added for that case without bringing back a
Python pipeline.

## Documents

Each stage writes the documents the CLI reference defines
([CLI reference](https://pqbench.github.io/pqbench/reference/cli/)). To inspect a stream
as tables while building a pipeline:

```console no-run
$ pqbench list catalogs | pqbench format
```

The records are typed NDJSON; the schema is the `pqbench.v1` model of each
resource, and the command reference describes the CLI-only documents.
