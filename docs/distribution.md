# Distribution

`pqbench` ships as prebuilt binaries through
[cargo-dist](https://opensource.axo.dev/cargo-dist/) (`dist`), published as
GitHub Releases. No Cargo registry is in the loop: crates.io cannot republish a
version, and GitHub Packages has no Cargo registry at all.

## Install

The newest release installs with the generated shell installer:

```sh
curl -LsSf https://github.com/pqbench/pqbench/releases/latest/download/pqbench-cli-installer.sh | sh
```

It detects the platform, downloads the matching archive, verifies the checksums,
and installs `pqbench` into `$CARGO_HOME/bin` (default `~/.cargo/bin`). Run it
with `--help` for the flags (pinning a version, choosing the install path, …).

The same release publishes the Python bindings to PyPI, at the same version:

```sh
pip install pqbench
```

To build from source instead — no registry, always current:

```sh
cargo install --git https://github.com/pqbench/pqbench pqbench-cli
```

Or, from a checkout:

```sh
cargo install --path crates/pqbench-cli
```

## Artifacts

Each release carries one archive per target plus a checksum beside it, a combined
`sha256.sum`, the shell installer, and a source tarball:

```
pqbench-cli-<target>.tar.xz
pqbench-cli-<target>.tar.xz.sha256
pqbench-cli-installer.sh
sha256.sum
source.tar.gz
```

| target | platform |
| --- | --- |
| `x86_64-unknown-linux-gnu` | Linux x86_64 |
| `aarch64-unknown-linux-gnu` | Linux arm64 |
| `x86_64-apple-darwin` | macOS Intel |
| `aarch64-apple-darwin` | macOS Apple silicon |

Assets are named after the crate (`pqbench-cli`) — cargo-dist's convention, not
the binary name (`pqbench`). The archive holds the binary and the licenses. The
newest release can be fetched directly, or a specific tag pinned:

```
https://github.com/pqbench/pqbench/releases/latest/download/pqbench-cli-x86_64-unknown-linux-gnu.tar.xz
https://github.com/pqbench/pqbench/releases/download/v0.0.1/pqbench-cli-x86_64-unknown-linux-gnu.tar.xz
```

Linux binaries are built on `ubuntu-22.04`, so they run on glibc 2.35 or newer
(Ubuntu 22.04, Debian 12, RHEL 9 and up).

Each archive also carries a Sigstore-backed build-provenance attestation
(`github-attestations`), so a download can be checked against the workflow that
produced it, not just against a checksum:

```sh
gh attestation verify pqbench-cli-x86_64-unknown-linux-gnu.tar.xz --repo pqbench/pqbench
```

## What is in the binary

Every target is built `--all-features`: `aws`, `delta`, and `iceberg` are
enabled, so `s3://` inputs and Delta/Iceberg tables work out of the box. A
downloaded binary cannot be rebuilt by its user, so a partial build that fails
at runtime on remote input is not worth shipping. The cost is size — roughly
49 MiB on disk and 19 MiB for the download (x86_64), dominated by delta-rs and
the AWS SDK. The feature sets are additive, so the intermediate combinations
(`aws` or `delta` alone) are subsets of `--all-features` and are not separate
artifacts.

The build uses a portable CPU baseline: `.cargo/config.toml` sets the codec
flags without `-march=native`, so the codecs are not tuned to the build machine.
These numbers are benchmarks — a `SIGILL` on an older CPU is a worse failure
than a codec a few percent slower. To reproduce lzbench's native numbers locally,
export `CXXFLAGS` with `-march=native` for that build.

## Releasing

Configuration lives in [`dist-workspace.toml`](../dist-workspace.toml), and the
workflow (`.github/workflows/release.yml`) is generated from it — change the
config and run `cargo dist generate`, never the workflow by hand.

Releases are cut by hand, not on every merge. Three steps.

**1. Bump the version** in `[workspace.package]` in `Cargo.toml`, carry it into
the wheel's manifest, commit, and merge it — the release tag must equal this
version:

```sh
# Cargo.toml: version = "0.0.2"
make sync-version
git commit -am "release: 0.0.2"
```

**2. Dispatch the binary release** with the matching tag:

```sh
gh workflow run release.yml -f tag=v0.0.2
```

or run the **Release** workflow from the Actions tab with `tag: v0.0.2`. The
default input, `dry-run`, runs only `plan` (no build, no publish), so it previews
what a release would produce. If the tag does not match the crate version,
`dist` errors before building.

`dist` derives the release from the tag, creates the GitHub Release (and the git
tag) itself, then runs `plan → build-local-artifacts → build-global-artifacts →
host → announce`: it builds each target, generates the installer, checksums and
source tarball, and uploads them. Every version is kept — cargo-dist does not
prune — and each is published as "latest", so consumers' `releases/latest/...`
URL always serves the newest.

**3. Dispatch the wheel** once that tag exists:

```sh
gh workflow run publish-pypi.yml
```

or run the **Publish Python package** workflow from the Actions tab. Publishing
is separate because PyPI names the workflow file that may upload, and its
trusted publisher does not accept a job defined in a workflow that another
workflow calls (`publish-pypi.yml` is called, not triggered, from `dist`'s
release run). It builds the wheels and the sdist and uploads them under trusted
publishing: no token is stored.

The version is the workspace's, so the two dispatches cannot ship different
versions. `python/` is its own workspace root and cannot inherit
`[workspace.package]`; `scripts/version.sh` is the one definition of the value —
`make sync-version` writes it, `make check-version` (part of `make check`) fails
when the two disagree, and the wheel jobs run the same script before maturin
reads the manifest.

## Source consumers

To track `main` from a source dependency, set in `Cargo.toml`
`pqbench = { git = "https://github.com/pqbench/pqbench", branch = "main" }` and
run `cargo update -p pqbench` to move the lock to the newest commit.
