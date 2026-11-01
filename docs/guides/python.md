# Python orchestration

`pqbench` is a binary, not a Python library. That is deliberate: the CLI owns
the concurrency, the NDJSON documents are the interface, and any Python process
can drive it. This guide shows how to keep the shell's streaming when the
driver is Python, and when a pipeline library is worth the extra dependency.

The examples measure Parquet files. The same patterns work for a Delta or
Iceberg table: replace `pqbench table-file` with `pqbench list files` and feed
the table ref on stdin.

## Measure one file

The repository has two runnable versions of this walk under
[`examples/python/`](https://github.com/pqbench/pqbench/tree/main/examples/python).
The standard-library one starts `pqbench table-file` and `pqbench bytemass` with
`Popen`, connects them with an OS pipe, and folds each record as it arrives. No
intermediate file, no full list in memory.

```console run
$ python3 examples/python/measure_files.py examples/quickstart.parquet
files,rows,column_chunks,file_bytes,column_bytes
1,8,2,541,170
```

`file_bytes` is the size of the Parquet files; `column_bytes` is the sum of the
byte masses `list columns` measured.

```python
import subprocess

PQB = "pqbench"

names = subprocess.Popen(
    [PQB, "table-file", "examples/quickstart.parquet"],
    stdout=subprocess.PIPE,
)
mass = subprocess.Popen(
    [PQB, "bytemass", "--fan-out", "128"],
    stdin=names.stdout,
    stdout=subprocess.PIPE,
)
# The parent must drop its copy of the producer's stdout, or the consumer never
# sees EOF after the producer exits.
names.stdout.close()

for line in mass.stdout:
    ...  # parse one NDJSON record as it arrives

mass_status = mass.wait()
names_status = names.wait()
if mass_status != 0 or names_status != 0:
    raise SystemExit("the walk failed")
```

Two details matter more than they look:

- Close `names.stdout` in the parent. The child holds its own copy; the parent's
  copy keeps the pipe open and the consumer waits forever.
- Check both return codes. A downstream stage can exit 0 after an earlier stage
  failed, so the last status is not the pipeline's status.

## Three ways to compose commands

Each snippet keeps the binary in `PQB`; point it at the wheel's `bin/pqbench`
when the environment's `bin/` is not on `PATH`.

### 1. Let bash own the pipe

The shortest option. Python starts one shell, the shell runs the pipeline you
would type, and Python reads the artifact.

```python
import subprocess

PQB = "pqbench"
script = f"""
set -euo pipefail
{PQB} table-file examples/quickstart.parquet | {PQB} bytemass > bytemass.ndjson
"""
subprocess.run(["bash", "-o", "pipefail", "-c", script], check=True)
```

Use this when the pipeline is fixed, the inputs are trusted, and you want the
CLI's exact semantics. `pipefail` is not optional: without it, `bytemass` sees
EOF from a failed `table-file`, writes a partial stream, and exits 0.

The caveats are the shell's: quoting, no Windows, and a table name interpolated
into the script is an injection risk. Use the next pattern for untrusted input.

### 2. Build the pipe with `Popen`

`Popen` is the standard-library streaming API. Build the chain once, then read
the last stage:

```python
import subprocess
from collections.abc import Iterator

PQB = "pqbench"


def pipeline(*commands: list[str]) -> Iterator[bytes]:
    previous = None
    processes = []
    for command in commands:
        process = subprocess.Popen(
            command,
            stdin=previous.stdout if previous else None,
            stdout=subprocess.PIPE,
        )
        if previous:
            previous.stdout.close()
        processes.append(process)
        previous = process

    try:
        for line in previous.stdout:
            yield line
    finally:
        previous.stdout.close()
        statuses = [process.wait() for process in processes]
    for process, status in zip(processes, statuses):
        if status != 0:
            raise SystemExit(f"{' '.join(process.args)} exited {status}")


for line in pipeline(
    [PQB, "table-file", "examples/quickstart.parquet"],
    [PQB, "bytemass", "--fan-out", "128"],
):
    ...
```

No shell, no quoting, argv-safe, and the stages still overlap. The cost is the
pipe bookkeeping above: close every parent copy of a stdout, and wait for every
process, not just the last one.

### 3. Use plumbum

[plumbum](https://plumbum.readthedocs.io/) turns the same walk into a `|`
expression. It is an optional dependency; the wheel does not ship it.

```python
from plumbum import local

PQB = local["pqbench"]
pipeline = PQB["table-file", "examples/quickstart.parquet"] | PQB["bytemass", "--fan-out", "128"]

process = pipeline.popen()
for line in process.stdout:
    ...  # bytes, one NDJSON record per line

if process.wait() != 0:
    raise SystemExit("the walk failed")
```

`pipeline.run()` buffers and returns `(status, stdout, stderr)`; `.popen()`
streams; calling the pipeline (`pipeline()`) returns stdout. Build the pipeline
lazily, then start it once. Iterate `process.stdout`, not the pipeline object.

## Which pattern?

| pattern | code | streams | extra dependency | failure |
| --- | --- | --- | --- | --- |
| bash `-c` | one call | yes | none | `pipefail` + `check=True` |
| `Popen` chain | ~20 lines | yes | none | check every `wait()` |
| plumbum | ~5 lines | yes | plumbum | `run()` raises; check `wait()` |
| `subprocess.run` per stage | one call per stage | no | none | `check=True` per stage |

The last row is the one to avoid for large stages: each `run` waits for the
previous stage to exit, so a four-stage walk becomes four barriers. The other
three patterns let the stages overlap.

## A full walk: catalog to files

The same patterns carry a whole catalog walk. This one lists every table a
workspace exposes, keeps the credential-eligible ones, and writes their active
files as a compressed NDJSON artifact.

The repository's `scripts/dbx-walk.sh` is the full version: it adds a `jq`
filter before `credentials check` so the walk descends only the catalog the
principal may read outside compute. On a workspace you do not own, that filter
is required, not optional. The block below is the shape of the walk, not a
copy-paste live run: it is the same pipeline without the filter.

```python
import subprocess

PQB = "pqbench"
script = f"""
set -euo pipefail
{PQB} list catalogs \
  | {PQB} ratelimit --rate 15 \
  | {PQB} list schemas \
  | {PQB} ratelimit --rate 15 \
  | {PQB} list tables \
  | {PQB} ratelimit --rate 15 \
  | {PQB} credentials check \
  | {PQB} credentials get \
  | {PQB} get table \
  | {PQB} list files \
  | zstd -3 -T1 > .files.jsonl.zst
mv .files.jsonl.zst files.jsonl.zst
"""
subprocess.run(["bash", "-o", "pipefail", "-c", script], check=True)
```

Each `ratelimit` stage paces the catalog requests downstream of it: `list
schemas`, `list tables`, and the table stages from `credentials check` on. The
catalog endpoints allow about 15 requests per second, and every record at these
levels is one request. `credentials check` keeps the tables whose credentials
the endpoint will vend (it filters by eligibility, not by grant); `credentials
get` and `get table` attach the vended lease. The hidden temp name plus `mv`
keeps a reader from seeing a partial file; it is required only when something
reads the directory while the pipeline runs.

## Read the stream

The walk above writes `files.jsonl.zst`, a uniform stream of `TableFile`
records. Read it with the engine you already use:

```python
import polars as pl

files = pl.scan_ndjson("files.jsonl.zst")
```

```python
# Spark needs the zstd Hadoop codec for this file.
files = spark.read.json("files.jsonl.zst")
```

```python
import duckdb

duckdb.sql("select * from read_json_auto('files.jsonl.zst')")
```

The measurement stream (`bytemass`) mixes one `Blob` per file with one
`Column` per column, its byte mass nested under `mass`. Parse it as it arrives,
or render it with `pqbench format`:

```console no-run
$ pqbench table-file examples/quickstart.parquet | pqbench bytemass | pqbench format
```

## Pitfalls

- **`pipefail` is not optional.** Without it, a downstream `zstd` exits 0 on a
  truncated stream. Set it in the shell, or check every process's status in
  Python.
- **`subprocess.run` per stage is a barrier.** It is fine for small inputs and
  a poor fit for a walk: it loses the overlap between listing and measuring.
- **Close parent pipe ends.** A parent's copy of a child's stdout keeps the
  pipe open; the consumer waits for EOF that never comes.
- **Auth is environment, not argv.** `PQB_ENDPOINT` / `PQB_TOKEN` are read
  from the process environment, which `Popen` inherits. Tokens in argv are
  visible in `ps`.
- **Do not add threads on top of `--fan-out`.** `bytemass` already runs 128
  files in flight; `list files` runs 64 tables. Python threads only add
  contention.
- **NDJSON is the pipe format.** `pqbench format` is for humans; parse the
  records, not the rendered table.
- **A partial stream can still exit 0.** Check the status of the stage that
  produced the records, not only the stage that consumed them.
