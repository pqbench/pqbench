# pqbench

PyO3 bindings for every [pqbench](https://github.com/pqbench/pqbench) command.
`pip install` builds a native wheel. Each function calls the same library
entry point as the matching CLI subcommand.

## Install

```sh
pip install pqbench
```

From a checkout:

```sh
pip install maturin
maturin develop --manifest-path python/Cargo.toml
```

The default build includes Delta (`pqbench.table`). Iceberg and `s3://` reads
need a rebuild (`maturin develop --features iceberg` / `--features aws`).

## Commands

```python
import pqbench

pqbench.lz("file.bin", codecs=["zstd@3"], samples=10, json=True)
pqbench.compression("data.parquet", per_column=True, json=True)
rows = pqbench.bytemass("data.parquet")          # list of bytemass-row dicts
pqbench.table("./delta-table")                   # pqbench.table document
pqbench.lake("./warehouse")                      # pqbench.lake document
pqbench.dump("dump-dir", info)                    # copy the table's files
pqbench.viz(rows, output="report")               # report.html
```

`pqbench.commands` is `("lz", "compression", "bytemass", "table", "lake",
"dump", "viz")`.

`codecs` repeats `codec@level`. Defaults match the CLI: `samples` 10,
`warmup_iterations` 3, `mode` `fastest`. `mode` is `fastest` or `mean`.
`table` and `lake` always return the versioned document. `dump` accepts a table
URI or those documents and copies the Parquet files a table or lake names into
the output directory, returning `file_count` and `byte_count`. `viz` collects a
bytemass row list into a static HTML treemap. `env` on `bytemass` and `table` may
only contain `AWS_*` names.

## Publish to PyPI

[`.github/workflows/publish-pypi.yml`](../.github/workflows/publish-pypi.yml)
is the release template. It builds manylinux, macOS, and Windows wheels with
[maturin](https://www.maturin.rs/) and uploads them with
[PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/).

Before the first upload:

1. On PyPI, add a pending publisher for this repository: owner `pqbench`,
   project `pqbench`, workflow `publish-pypi.yml`, environment `pypi`.
2. In the GitHub repository settings, create an environment named `pypi`.
3. Run the **Publish Python package** workflow, or publish a GitHub Release.

The workflow runs only in `pqbench/pqbench`. A fork dispatch does not upload.
