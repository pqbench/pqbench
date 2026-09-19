# pqbench

*lzbench for parquet.*

Measure how well each codec compresses a parquet file and how many on-disk bytes
each column costs.

## Quick start

Requires Rust 1.91.1+ and [git-lfs](https://git-lfs.com) (for the parquet
fixtures):

```sh
git clone https://github.com/pqbench/pqbench.git
cd pqbench
cargo run -p pqbench-cli -- bytemass crates/pqbench/tests/fixtures/small_snappy.parquet
```

This prints the per-column byte masses, read from the footer only:

```
bytemass: small_snappy.parquet
column                              bytes/row
a                                        0.38
--------------------------------------------
total                                    0.38
```

For your own data, `make samples` fetches a few open datasets into
`local/samples/`.

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
compression. Multiple paths and quoted glob masks are aggregated:

```sh
pqbench bytemass data.parquet
pqbench bytemass 'data/part-*.parquet'
```

`--json` emits a composable `{name, value, children}` tree; `--d3` emits a
self-contained HTML treemap:

```sh
pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
```

### delta

Byte-mass summary of a local Delta table snapshot. Feature-gated — build with
`--features delta` to get the command:

```sh
pqbench delta ./path/to/table
```

## Documentation

- [Delta tables](docs/delta.md) — snapshot resolution, report shape, limitations
- [Docker](docs/docker.md) — build, run, and publish a container image

## Contributing

PRs welcome. The gate is `make check` (`fmt-check` + `clippy -D warnings` +
`test`) and every change must pass it. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the loop, style, and naming rules.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).
