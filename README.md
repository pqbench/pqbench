# pqbench

*lzbench for parquet.*

Measure how well each codec compresses a parquet file and how many on-disk bytes
each column costs.

## Quick start

Docker is the fastest way to try it — no Rust toolchain, no build:

```sh
docker pull pqbench/pqbench:latest
docker run --rm -v "$PWD:/data:ro" pqbench/pqbench:latest bytemass /data/your.parquet
```

The published image is a portable baseline build; see
[docs/docker.md](docs/docker.md) for how its numbers compare to a native build.

## Commands

### lz

lzbench-style compression benchmark over raw file bytes:

```sh
pqbench lz file.bin -c zstd@3 --samples 10
```

### compression

The same codec sweep over the encoded pages of a **NONE-compressed** parquet
file:

```sh
pqbench compression data.parquet --per-column
```

### bytemass

Per-column byte masses — how many on-disk bytes each column takes per row.
Reads only the footer metadata, so it works on any file regardless of
compression. Multiple paths, quoted glob masks, and storage URIs are
aggregated:

```sh
pqbench bytemass data.parquet
pqbench bytemass 'data/part-*.parquet'
pqbench bytemass s3://bucket/table/part-0.parquet   # requires --features aws
```

`--json` emits a composable `{name, value, children}` tree; `--d3` emits a
self-contained HTML treemap:

```sh
pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
```

Remote reads fetch the object metadata, the Parquet trailer, and the
serialized footer — never the data pages. `s3://` support is the `aws`
feature; a URI whose backend is not compiled in fails at runtime with the
missing feature named. The library it uses (`bytemass::read_remote`,
`bytemass::summarize_inputs`) is always available and never feature-gated.

### delta

Byte-mass summary of a local Delta table snapshot. Feature-gated — build with
`--features delta` to get the command:

```sh
pqbench delta ./path/to/table
```

Add `--features delta-s3` to resolve and measure Delta tables at `s3://` URIs;
the active files are measured through `bytemass`'s public remote reader.

## Documentation

- [Local lakehouse E2E examples](docker/e2e-lakehouse/README.md) — LocalStack S3,
  Unity Catalog, Iceberg REST and DuckLake piped into `bytemass --source -`
- [Delta tables](docs/delta.md) — snapshot resolution, report shape, limitations
- [Docker](docs/docker.md) — build, run, and publish a container image

## Contributing

PRs welcome. The gate is `make check` (`fmt-check` + `clippy -D warnings` +
`test`) and every change must pass it. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
