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
reads and Delta/Iceberg tables work out of the box. Storage reads authenticate
with the `env=` argument (`AWS_*` names) or the process environment.

From a checkout:

```sh
pip install maturin
maturin develop --manifest-path python/Cargo.toml
```

## The commands

Each command is the library function of the same path, under `pqbench.native`,
and takes the handles and the resource's name like the Rust signature: the name
is the AIP resource name a parent record carries, so a record feeds the next
command as it is. `credentials.get` returns the `AWS_*` lease map, and a record
is its model's class: `record["uri"]`, `record.uri` and `record.asdict()` agree.

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
    # `credentials.get.get` is ref -> ref: the lease lands on the ref as `credentials`.
    ref = await pqbench.native.credentials.get.get(workspace, refs[0])
    storage = pqbench.native.Storage(credentials=ref["credentials"])
    table = await pqbench.native.table.get.get(
        workspace,
        lake,
        "catalogs/dbx_samples/schemas/nyctaxi/tables/trips",
        storage,
    )

    # `partition.list.list` and `file.list.list` stream (async iterators); the
    # single-value verbs are awaitable.
    async for partition in pqbench.native.partition.list.list(
        lake, table["name"], table["uri"], storage.env, 86_400_000
    ):
        async for file in pqbench.native.file.list.list(
            lake, partition["uri"], storage.env, partition["commit_versions"]
        ):
            print(file["uri"])

asyncio.run(main())
```

### The walk

`walk` is the one composition rather than a binding: it spans the catalog
levels and descends every table the principal may read outside compute, with
the tables in flight together (bounded by `concurrency`, 32 by default), and
yields each table file.

```python
async for file in pqbench.walk(workspace):
    print(file["uri"])
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

The analysis commands are synchronous and take their Rust request structs:

```python
report = pqbench.native.lz.lz(pqbench.native.LzRequest(file="file.bin", codec_specs=["zstd@3"]))
report = pqbench.native.compression.compression(
    pqbench.native.CompressionRequest(file="data.parquet", per_column=True)
)
sample = pqbench.native.third_party.parquet.read_sample("data.parquet", None)
pqbench.native.profile.profile(sample, pqbench.native.ProfileRequest(top=5))
typed = pqbench.native.third_party.parquet.read_typed_sample("data.parquet", 512)
pqbench.native.experiment.experiment(typed, pqbench.native.ExperimentRequest(trials=["sort:text"]))
pqbench.native.diff.compare(rows, rows, 2)
pqbench.native.skill.document("parquet-advisor", "recipes")
pqbench.native.viz.write_report("report", rows)
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

`env` is the library's `BTreeMap<String, String>` and is passed straight
through, so a vended lease (`credentials.get`) or the process environment rides
as given. `codec_specs` repeats `codec@level`; `samples` defaults to 10,
`warmup_iterations` to 3, `mode` to `Mode.FASTEST`.

## Publish to PyPI

[`.github/workflows/publish-pypi.yml`](../.github/workflows/publish-pypi.yml)
builds manylinux, macOS, and Windows wheels with
[maturin](https://www.maturin.rs/) and uploads them with
[PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/). It runs
only on a manual dispatch (and only in `pqbench/pqbench`): the binary release
is cut separately, and PyPI versions are immutable.

Before the first upload:

1. On PyPI, add a pending publisher for this repository: owner `pqbench`,
   project `pqbench`, workflow `publish-pypi.yml`, environment `pypi`.
2. In the GitHub repository settings, create an environment named `pypi`.
3. Run the **Publish Python package** workflow.

The wheel version comes from `python/Cargo.toml` (`version`), which is kept in
step with the workspace release by hand — `python/` is its own crate and cannot
inherit `[workspace.package]`.
