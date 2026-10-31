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

The shortest version lets bash own the pipe:

```python
import subprocess

subprocess.run(
    [
        "bash",
        "-o",
        "pipefail",
        "-c",
        "pqbench table-file examples/quickstart.parquet "
        "| pqbench bytemass > bytemass.ndjson",
    ],
    check=True,
)
```

`pipefail` is what makes the pipeline fail when an earlier stage does: without
it, `bytemass` sees EOF from a dead producer, writes a partial stream, and exits
0. The child inherits the environment; keep `PQB_ENDPOINT` / `PQB_TOKEN` there,
not in argv.

`subprocess.run` is one command at a time. When you want the records in Python
as they arrive, use `subprocess.Popen` or a pipeline library such as
[plumbum](https://plumbum.readthedocs.io/). The
[Python orchestration guide](https://pqbench.github.io/pqbench/guides/python/)
walks through all three patterns and the trade-offs; the repository ships
runnable versions under
[`examples/python/`](https://github.com/pqbench/pqbench/tree/main/examples/python).

Then read the measurement stream. It mixes one `Blob` per file with one
`Bytemass` per column, so filter on the fields you need:

```python
import json

with open("bytemass.ndjson") as stream:
    for line in stream:
        record = json.loads(line)
        if "compressed_bytes" in record:  # a column measurement
            print(record["name"], record["compressed_bytes"])
```

The file-list artifact (`pqbench list files`) is uniform and reads directly
with polars, DuckDB, or Spark; the
[Python orchestration guide](https://pqbench.github.io/pqbench/guides/python/)
shows those.

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
