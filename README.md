# pqbench

*lzbench for parquet.*

`pqbench` is a small command-line tool for measuring parquet (and raw file)
compression behavior: how well each codec compresses a file's bytes, how fast it
compresses/decompresses them, and how big each column is on disk. It is
dependency-light, reads only what it needs, and keeps measurement separate from
presentation so its output can be fed to other tools.

## Build

```
cargo build --release
```

The gate is `make check` (`cargo fmt --check`, `clippy -D warnings`, and the
test suite). `make samples` fetches a few open parquet datasets into
`data/samples/` for manual testing.

## Docker

Build, run, and publish a container image of the release binary (no local Rust
toolchain needed). See [docs/docker.md](docs/docker.md) for usage, the
multi-arch workflow, and Docker Hub release setup.

## Commands

```
pqbench <COMMAND> [OPTIONS]
```

### lz

lzbench-style compression benchmark over raw file bytes:

```
pqbench lz file.bin -c zstd@3 --samples 10
```

Sweeps every wired codec (gzip, lz4, snappy, zstd) over the file at each level,
reporting the compression ratio and throughput in megabytes per second. Use
`-c codec@level` to restrict the sweep and `--mode`/`--samples`/`--warmup-iterations`
to tune the measurement.

### compression

The same sweep over the encoded pages of a **NONE-compressed** parquet file:

```
pqbench compression data.parquet --per-column
```

`--per-column` adds a per-column breakdown. This command decodes the page
payloads and re-compresses them, so the input must be uncompressed (NONE).

### bytemass

Per-column byte masses — how many on-disk bytes each column takes per row:

```
pqbench bytemass data.parquet
```

This reads **only the parquet footer metadata**, so it works on any file
regardless of column compression and never loads the pages into memory.

- `--json` — emit the byte-mass tree as composable `{name, value, children}` JSON
  for a downstream tool.
- `--d3` — emit a self-contained HTML page that renders the masses as a treemap
  (d3 imported as ES modules from a CDN):

```
pqbench bytemass data.parquet --d3 > treemap.html && xdg-open treemap.html
```
