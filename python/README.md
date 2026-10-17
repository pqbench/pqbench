# pqbench

PyO3 bindings for [pqbench](https://github.com/pqbench/pqbench). `pip install`
builds a native wheel; each function calls the same library entry point as the
matching CLI subcommand, and a separate `pqbench` binary is not required.

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

## Commands

The walk is verb-first, mirroring `pqbench get …` and `pqbench list …`: each
`get_*` reads one resource and each `list_*` enumerates a collection. They
return the `pqbench.v1` wire types as dicts, so a value round-trips into the
next call or a CLI stage.

```python
import pqbench

# The catalog walk. Needs an endpoint: pass endpoint=/token= or set
# PQB_ENDPOINT / PQB_TOKEN (DATABRICKS_HOST / DATABRICKS_TOKEN also work).
pqbench.get_metastore()
pqbench.get_catalog("dbx_samples")
pqbench.get_schema("dbx_samples", "nyctaxi")
pqbench.get_table("dbx_samples", "nyctaxi", "trips")
pqbench.list_catalogs()
pqbench.list_schemas("dbx_samples")
pqbench.list_tables("dbx_samples", "nyctaxi")

# The storage levels read a table by path; no endpoint is needed.
partitions = pqbench.list_partitions("./events", every="1d")
files = pqbench.list_files(partitions[0])       # the window's added files

# Credentials: gate on the catalog, then attach the vended lease.
tables = pqbench.list_tables("dbx_samples", "nyctaxi")
tables = pqbench.credentials_check(tables)       # drop what compute reads
tables = pqbench.credentials_get(tables)         # attach the AWS_* lease

# Analysis.
pqbench.lz("file.bin", codecs=["zstd@3"], samples=10, json=True)
pqbench.compression("data.parquet", per_column=True, json=True)
rows = pqbench.bytemass("data.parquet")          # list of bytemass-row dicts
pqbench.diff(rows, other_rows, depth=2)          # two row lists, or two files
pqbench.profile("data.parquet", rows="first:8192")
pqbench.experiment("data.parquet", rewrite=["sort:text"])
pqbench.ratelimit(rows, rate=15.0)               # pace a record stream
pqbench.viz(rows, output="report")               # report.html
pqbench.skill("parquet-advisor", "recipes")      # the writer's recipes
```

`pqbench.commands` lists every bound name.

## Configuration

The endpoint and token keyword arguments mirror the CLI's global flags and
resolve in the same order: the argument, then `PQB_ENDPOINT` / `PQB_TOKEN` /
`PQB_TABLE_FORMAT`, then `DATABRICKS_HOST` / `DATABRICKS_TOKEN`. `table_format`
is `unity` (the default) or `iceberg`.

`env=` on `bytemass`, `get_table`, `list_partitions`, `list_files`, and `diff`
may only contain `AWS_*` names. `setup()` returns the environment the CLI's
`setup` exports, as a dict.

`codecs` repeats `codec@level`; `samples` defaults to 10, `warmup_iterations`
to 3, `mode` to `fastest` (`fastest` or `mean`).

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
