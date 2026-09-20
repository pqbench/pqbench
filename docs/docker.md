# Docker

Build a container image containing the release binary, so you can run pqbench
without a local Rust toolchain:

```sh
docker build -t pqbench:local .
docker run --rm pqbench:local --help
docker run --rm -v "$PWD:/tmp:ro" pqbench:local bytemass /tmp/file.parquet
docker run --rm -v "$PWD:/tmp:ro" pqbench:local bytemass /tmp/file.parquet --json > masses.json
docker run --rm -v "$PWD:/tmp:ro" pqbench:local bytemass /tmp/file.parquet --d3 > treemap.html
docker run --rm -v "$PWD:/tmp:ro" pqbench:local lz /tmp/file.bin -c zstd@3
docker run --rm -v "$PWD:/tmp:ro" pqbench:local compression /tmp/uncompressed.parquet --per-column
```

## Runtime image

- Runs as UID/GID `65532:65532`. Mounted input files must be readable by that
  user; on Linux, `--user "$(id -u):$(id -g)"` uses your own file permissions.
- Shell redirects write output on the host.
- `compression` still requires NONE-compressed Parquet input.
- The image ships only the binary and its statically linked codecs (snappy,
  zstd, lz4, zlib); the Rust toolchain lives in a separate build stage. It uses
  musl libc, so it runs on any Linux regardless of the host glibc.
- Built with `--no-default-features` (core): the `aws`, `delta` and `delta-s3`
  features are not included, so `s3://` inputs and Delta tables are unavailable
  in the container.

## Smoke tests

The Docker workflow builds and smoke-tests Linux AMD64 and ARM64 images. Run the
same smoke tests locally against your build:

```sh
sh scripts/test_docker.sh pqbench:local
```

(requires Docker and Python 3).

## Performance and benchmark numbers

Benchmark results always depend on the CPU that runs them — there is no single
"correct" number. Two things shape the numbers you get from a container image:

1. **libc** — the published image uses musl (static, portable). A native glibc
   build can differ by a few percent.
2. **CPU baseline** — the published image compiles the codecs for a portable
   baseline instead of `-march=native`, so it runs on a wide range of CPUs:
   `-march=x86-64-v3` on AMD64, `-mcpu=neoverse-n1` on ARM64.

The published image is a "baseline" build: it runs everywhere, but its numbers
are baseline numbers, not your CPU's best.

For **exact host numbers**, build the image natively on the hardware you care
about. This compiles with `-march=native` (and `-C target-cpu=native`), so it is
tuned to that specific CPU and is only safe to run on it:

```sh
docker build --build-arg NATIVE=1 -t pqbench:native .
docker run --rm -v "$PWD:/tmp:ro" pqbench:native bytemass /tmp/file.parquet
```

Use the baseline image for "does it work" and convenience; use a `NATIVE=1`
build on the target machine when you need numbers you can compare across runs.

## Publishing strategy and tradeoffs

One multi-arch image (`amd64` + `arm64` under a single tag) is published to
Docker Hub on a GitHub release. For now it ships as `latest` only, with no
version tags. The build uses musl (Alpine) and a per-arch portable CPU baseline
(`-march=x86-64-v3` / `-mcpu=neoverse-n1`), with a `--build-arg NATIVE=1` escape hatch for
exact-host builds.

- **Multi-arch as one tag** — one `docker pull` works on any machine. Cost: a
  single image serves both arches, so per-arch CPU tuning is the only lever; you
  can't get an "amd64-only" fast path without splitting tags.
- **`latest` only** — always newest, zero versioning work. Cost: not
  reproducible (two pulls on different days differ), no rollback, no pinning. For
  a benchmark tool that matters, since users can't reproduce or compare numbers
  across time.
- **musl vs glibc** — tiny (~5 MB), static, runs anywhere. Cost: benchmark
  numbers differ slightly from a native glibc build; ties you to Alpine.
- **Portable baseline vs `native`** — runs on any CPU (no SIGILL). Cost: it's a
  baseline build, so numbers are "baseline," not your CPU's best. `NATIVE=1`
  fixes that but only on the machine that builds it.
- **Docker Hub secrets** (`DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN`) — kept as
  placeholders, only read in the release-gated publish job. Cost: a maintainer
  must configure them before the first publish.

The central tradeoff is **portability ⇄ accurate numbers**: you can have one
artifact that runs on everyone's machine (baseline numbers) or a per-machine
artifact (native numbers) — not both. The resolution chosen is to publish a
portable image for convenience and document the `NATIVE=1` / local-build path for
measurement.
