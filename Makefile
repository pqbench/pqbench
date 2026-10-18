# Standard Rust workspace developer/orchestration targets.
# Overridable without modifying this file:
#   CARGO          (default: cargo)
#   CARGO_FEATURES (default: empty) feature selection, e.g. --all-features
#   TEST_FLAGS     (default: empty) test-harness args after `--`, e.g. --include-ignored
# Examples:
#   make build CARGO_FEATURES="--features aws"
#   make test  CARGO_FEATURES=--all-features TEST_FLAGS=--include-ignored

CARGO ?= cargo
CARGO_FEATURES ?=
TEST_FLAGS ?=
PYTHON ?= python3
# rust-code-analysis CLI and inputs; see the `code-analysis` target.
RCA ?= rust-code-analysis-cli
CODE_ANALYSIS_PATHS ?= crates
LAKEHOUSE = CARGO="$(CARGO)" ./docker/e2e-lakehouse/lakehouse.sh

.PHONY: all fmt fmt-check build test lint cache-stats samples lakehouse dbx-e2e \
	dbx-perf-walk lakehouse-up lakehouse-seed-s3 lakehouse-seed-unity \
	lakehouse-seed-iceberg check isolation lfs-check check-python sync-docs \
	check-docs check-version sync-version code-analysis clean

all: fmt build test lint

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

build:
	$(CARGO) build --workspace $(CARGO_FEATURES)

test:
	$(CARGO) test --workspace $(CARGO_FEATURES) $(if $(TEST_FLAGS),-- $(TEST_FLAGS))

# The live Databricks metastore e2e; needs the service principal credentials.
# The S3 features are on so the walk's storage reads actually run (under the
# vended keys; Databricks default storage refuses externally issued sessions).
dbx-e2e:
	@test -n "$$DBX_HOST" && test -n "$$DBX_SAMPLES_SP_CLIENT_ID" || { echo "set DBX_HOST and DBX_SAMPLES_SP_CLIENT_ID/SECRET (see docs/auth.md)"; exit 1; }
	$(CARGO) test -p pqbench-cli --features delta-s3,iceberg-s3 --test dbx_e2e -- --ignored --nocapture

# The live perf walk (scripts/dbx-perf-walk.sh): `perf stat`/`perf record` over
# the metadata walk. Needs DBX_HOST plus a bearer source — the service
# principal's client secret, or a ready PQB_ENDPOINT/PQB_TOKEN — and `perf`.
# Report-only; the `dbx-perf-walk` CI job runs it.
dbx-perf-walk:
	@test -n "$$DBX_HOST" || { echo "set DBX_HOST (see docs/auth.md)"; exit 1; }
	$(CARGO) build --release -p pqbench-cli --features delta-s3,iceberg-s3
	PQB="$(or $(CARGO_TARGET_DIR),$(CURDIR)/target)/release/pqbench" ./scripts/dbx-perf-walk.sh

lint:
	$(CARGO) clippy --workspace --all-targets $(CARGO_FEATURES) -- -D warnings

cache-stats:
	@if command -v sccache >/dev/null 2>&1; then \
		sccache --show-stats; \
	else \
		echo "sccache is not installed; run: cargo install sccache"; \
	fi

# Verify the committed open-dataset parquet samples in data/samples/ (Git LFS).
samples:
	./scripts/check_samples.sh

# Local Unity Catalog and Iceberg REST, ready to query: see
# docker/e2e-lakehouse/README.md.
lakehouse: lakehouse-seed-s3 lakehouse-seed-unity lakehouse-seed-iceberg
	$(LAKEHOUSE) check

# Storage, a credential for Unity to vend, and Unity answering.
lakehouse-up:
	$(LAKEHOUSE) up

# The Delta table in docker/e2e-lakehouse/table/, uploaded to the object store.
lakehouse-seed-s3: lakehouse-up
	$(LAKEHOUSE) seed-s3

# That table, registered as an external Delta table in Unity Catalog.
lakehouse-seed-unity: lakehouse-up
	$(LAKEHOUSE) seed-unity

# The Iceberg table in docker/e2e-lakehouse/iceberg/, registered over REST.
# Depends on seed-s3 so the lakehouse bucket exists on a fresh stand.
lakehouse-seed-iceberg: lakehouse-seed-s3
	$(LAKEHOUSE) seed-iceberg

check: fmt-check check-docs check-version lint isolation lfs-check test

# `third_party` wrappers keep feature flags in impl.rs, never in api.rs.
# ISOLATION_FLAGS=--github renders GitHub workflow-command annotations.
ISOLATION_FLAGS ?=
isolation:
	./scripts/check_isolation.sh $(ISOLATION_FLAGS)

# Every file under an LFS filter must be committed as a pointer, not a raw git
# blob. A raw blob checks out (the smudge filter only warns) but never reaches
# the LFS server, so it is invisible until a fresh clone. git-lfs is a
# contributor prerequisite; see CONTRIBUTING.md.
lfs-check:
	git lfs fsck

# The wheel's version, and the one definition of it: python/ is its own
# workspace root, so python/Cargo.toml cannot inherit [workspace.package].
# `check-version` is part of `check`, so drift fails on the pull request.
VERSION = $(shell ./scripts/version.sh)

sync-version:
	./scripts/version.sh sync

check-version:
	./scripts/version.sh check

# python/ is its own crate (not a workspace member); share the repo target
# dir so it does not grow python/target. Local to python/.venv for PEP 668.
# Its lock is committed, as the workspace's is.
check-python:
	$(CARGO) fmt --manifest-path python/Cargo.toml -- --check
	CARGO_TARGET_DIR="$(or $(CARGO_TARGET_DIR),$(CURDIR)/target)" $(CARGO) clippy --manifest-path python/Cargo.toml --all-targets -- -D warnings
	test -d python/.venv || $(PYTHON) -m venv python/.venv
	python/.venv/bin/python -m pip install -q maturin
	. python/.venv/bin/activate && CARGO_TARGET_DIR="$(or $(CARGO_TARGET_DIR),$(CURDIR)/target)" maturin develop --manifest-path python/Cargo.toml
	python/.venv/bin/python -m unittest discover -s python/tests

# Turn the documented `pqbench ...` commands into Rust integration tests under
# crates/pqbench-cli/tests/gen_*.rs. Committed, so `make test` runs them like any
# other test; `make check-docs` fails if they are out of date with the Markdown.
DOCS ?= README.md docs/ skills/
sync-docs:
	$(CARGO) run -q -p docscheck-cli -- sync $(DOCS)
check-docs:
	$(CARGO) run -q -p docscheck-cli -- check $(DOCS)

# Software metrics (rust-code-analysis) over the source tree; prints the CLI's
# own metrics tree. Needs `rust-code-analysis-cli` on PATH — CI pins the
# release — or set RCA to a binary. Report-only: `make check` does not run it.
code-analysis:
	$(RCA) --metrics -p $(CODE_ANALYSIS_PATHS)

clean:
	$(CARGO) clean
