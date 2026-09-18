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

Build an image containing the release binary (no local Rust toolchain needed):

```sh
docker build -t pqbench:local .
docker run --rm pqbench:local --help
docker run --rm -v "$PWD:/data:ro" pqbench:local bytemass /data/file.parquet
docker run --rm -v "$PWD:/data:ro" pqbench:local bytemass /data/file.parquet --json > masses.json
docker run --rm -v "$PWD:/data:ro" pqbench:local bytemass /data/file.parquet --d3 > treemap.html
docker run --rm -v "$PWD:/data:ro" pqbench:local lz /data/file.bin -c zstd@3
docker run --rm -v "$PWD:/data:ro" pqbench:local compression /data/uncompressed.parquet --per-column
```

The image runs as UID/GID `65532:65532`. Mounted input files must be readable by
that user; on Linux, `--user "$(id -u):$(id -g)"` can use your own file permissions.
Shell redirects write output on the host. `compression` still requires
NONE-compressed Parquet input. The final Alpine image contains the binary and
runtime libraries only; the Rust toolchain stays in a separate build stage.
Images use musl libc and portable CPU settings rather than the local build's
`-march=native`, so benchmark results can differ from native glibc builds.

The Docker workflow builds and smoke-tests Linux AMD64 and ARM64 images on pushes,
pull requests, and published releases. Run the same smoke tests locally with
`sh scripts/test_docker.sh pqbench:local` (requires Docker and Python 3).

### Docker Hub release setup

Before publishing, maintainers must create the Docker Hub repository and configure
these GitHub repository settings under **Settings → Secrets and variables → Actions**:

| Setting | Kind | Value |
| --- | --- | --- |
| `DOCKERHUB_USERNAME` | Secret | Docker Hub account with push access |
| `DOCKERHUB_TOKEN` | Secret | Docker Hub access token with write permission |
| `DOCKERHUB_IMAGE` | Variable | Namespace/image, default `pqbench/pqbench` |

The workflow contains secret references only; do not put credentials in files or
Docker build arguments. Publication runs only for releases in `pqbench/pqbench`,
after both architecture smoke tests pass. Forks and pull requests only build/test.
Manual workflow runs also only build/test.

Publish a GitHub release tagged `vMAJOR.MINOR.PATCH` (for example, `v0.0.1`) to push
the multi-platform image tagged `0.0.1` and `latest`. Prerelease suffixes such as
`v0.0.2-rc.1` are supported and never update `latest`; releases marked as prereleases
also never update `latest`. Keep release tags immutable and aligned with the Cargo
workspace version. Once the first image has been published:

```sh
docker pull pqbench/pqbench:0.0.1
docker run --rm -v "$PWD:/data:ro" pqbench/pqbench:0.0.1 bytemass /data/file.parquet
# For reproducible use, replace the tag with @sha256:<published-manifest-digest>.
```

Substitute the configured `DOCKERHUB_IMAGE` if using a different namespace.

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
