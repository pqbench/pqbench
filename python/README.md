# pqbench

PyO3 bindings for [pqbench](https://github.com/pqbench/pqbench). `pip install`
builds a native wheel; a separate `pqbench` binary is not required. Two
surfaces, one wheel:

* **`pqbench.native`** — the commands: one function per library path, with the
  same arguments. Every name is the Rust function of the same path.
* **`pqbench`** — the ergonomic surface over them: the client handles every
  command takes (`Config`, `Dialect`, `Lake`, `Layer`, `Workspace`) and `walk`,
  the whole-metastore walk composed once.

## Install

```sh
pip install pqbench
```

The wheel carries every storage backend (`delta-s3`, `iceberg-s3`), so `s3://`
reads and Delta/Iceberg tables work out of the box. A storage read
authenticates with the `credentials=` argument (a `Credentials` record) or,
for `bytemass`, with `env` on its request — or with the process environment.

From a checkout:

```sh
pip install maturin
maturin develop --manifest-path python/Cargo.toml
```

## The commands

Each command is the library function of the same path, under `pqbench.native`,
and takes the handles and the resource's name like the Rust signature: the name
is the AIP resource name a parent record carries, so a record feeds the next
command as it is. `credentials.get` takes a table's name and returns its lease,
the `Credentials` record a storage read takes, and a record is its model's
class: `record["path"]`, `record.path` and `record.asdict()` agree.

They are the walk's own pipeline — metastore, catalog, schema, table,
credentials, partition, file, bytemass — and they stop at `bytemass`: the
commands downstream of a byte-mass stream (`viz`, `diff`, `profile`,
`experiment`), the standalone codec benchmarks (`lz`, `compression`) and
`skill` are separate tools, and are not bound.

```python
import asyncio
import pqbench
import pqbench.native

async def main():
    workspace = pqbench.Workspace(pqbench.Config("https://…", token="…"))
    lake = pqbench.Lake(workspace)

    # The walk, one function per resource. The I/O verbs are awaitable.
    await pqbench.native.metastore.get.get(workspace)
    await pqbench.native.catalog.get.get(workspace, "catalogs/dbx_samples")
    await pqbench.native.catalog.list.list(workspace)
    await pqbench.native.schema.list.list(workspace, "catalogs/dbx_samples")
    refs = await pqbench.native.table.list.list(
        workspace, "catalogs/dbx_samples/schemas/nyctaxi"
    )
    await pqbench.native.credentials.check.check(
        workspace, "catalogs/dbx_samples/schemas/nyctaxi/tables/trips"
    )
    # `credentials.get.get` is name -> lease: the lease is what a storage read
    # takes, from the table's name alone.
    credentials = await pqbench.native.credentials.get.get(workspace, refs[0]["name"])
    table = await pqbench.native.table.get.get(
        workspace,
        lake,
        refs[0]["name"], refs[0].get("uri"), credentials,
    )

    # `partition.list.list` and `file.list.list` stream (async iterators); the
    # single-value verbs are awaitable.
    async for partition in pqbench.native.partition.list.list(
        lake, table["name"], table["uri"], table.get("credentials"), 86_400_000
    ):
        async for file in pqbench.native.file.list.list(
            lake,
            table["name"],
            partition["uri"],
            partition.get("credentials"),
            partition["commit_versions"],
        ):
            # The pipeline stops here: `bytemass` measures the file's bytes.
            rows = await pqbench.native.bytemass.bytemass(
                lake, pqbench.native.BytemassRequest(inputs=[file.uri])
            )
            print(len(rows), file.uri)

asyncio.run(main())
```

### The walk

`walk` is the one composition rather than a binding: it spans the catalog
levels and descends every table the principal may read outside compute, with
the tables in flight together (bounded by `concurrency`, 32 by default), and
yields each table file.

```python
async for file in pqbench.walk(workspace):
    print(file.uri)
```

`catalog=` descends one catalog instead of every readable one. A table that
cannot be read is skipped (logged at `debug`), so a mixed metastore still
walks; records stream, so the walk holds no more than a bounded queue of them.

The Unity Catalog API limits requests per endpoint and per workspace, so its
calls are paced at `requests_per_second` — 15, one bucket per endpoint, and `0`
turns pacing off. The storage reads are not paced: object storage has no such
limit and retries its own. Anything skipped is counted in a `warning` when the
walk ends, so a rate limit shows up as a count rather than as silently missing
files.

`bytemass` is the last command: it reads what `file.list.list` yields — a
path or URI per file — and returns one `Bytemass` per column, its chunks
folded over the file's row groups.

```python
columns = await pqbench.native.bytemass.bytemass(
    lake, pqbench.native.BytemassRequest(inputs=["s3://…/part-00000.parquet"])
)
```

[`samples/walk.py`](samples/walk.py) is a runnable walk over the **whole
metastore**: it calls `walk`, spans every catalog and schema, descends the one
catalog the principal may read outside compute, and prints each table file.

```sh
PQB_ENDPOINT=https://<workspace>.cloud.databricks.com PQB_TOKEN=<bearer> \
  AWS_REGION=us-east-2 python samples/walk.py
```

## Configuration

`Config(endpoint, token=None, dialect=Dialect.UNITY)` carries the endpoint, the
bearer, and the dialect (`UNITY` or `ICEBERG`); `Layer(endpoint=None,
token=None, dialect=None).resolve()` combines configuration layers into a
`Config`.

`credentials` is a `Credentials` record — what `credentials.get` returns — or
the mapping of the same fields; a read given none falls back to the process
environment. A request's `env` is the library's `BTreeMap<String, String>`,
passed straight through. A `BytemassRequest` names `inputs` (paths or glob
masks) and `env` (the `AWS_*` options a remote read uses).

## Publish to PyPI

[`.github/workflows/release-pypi.yml`](../.github/workflows/release-pypi.yml)
builds manylinux and macOS wheels with
[maturin](https://www.maturin.rs/) and uploads them with
[PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/) — no token
is stored. It has its own dispatch, after the matching tag is released:
`gh workflow run release-pypi.yml`, or **Publish to PyPI** from the
Actions tab. Publishing is separate because PyPI names the workflow file that
may upload, and its trusted publisher does not accept a job defined in a
workflow that another workflow calls.

Its version is the workspace's: `scripts/version.sh sync` writes
`[workspace.package] version` from `Cargo.toml` into `python/Cargo.toml` before
maturin reads it, and `make check-version` fails when the two disagree —
`python/` is its own crate and cannot inherit it.

Before the first upload:

1. On PyPI, add a pending publisher for this repository: owner `pqbench`,
   project `pqbench`, workflow `release-pypi.yml`, environment `pypi`.
2. In the GitHub repository settings, create an environment named `pypi`.
3. Run the **Publish to PyPI** workflow.
