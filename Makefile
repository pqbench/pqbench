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
LAKEHOUSE = CARGO="$(CARGO)" ./docker/e2e-lakehouse/lakehouse.sh

.PHONY: all fmt fmt-check build test lint cache-stats samples lakehouse \
	lakehouse-up lakehouse-seed-s3 lakehouse-seed-unity lakehouse-seed-iceberg \
	check isolation lfs-check check-python clean

all: fmt build test lint

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

build:
	$(CARGO) build --workspace $(CARGO_FEATURES)

test:
	$(CARGO) test --workspace $(CARGO_FEATURES) $(if $(TEST_FLAGS),-- $(TEST_FLAGS))

lint:
	$(CARGO) clippy --workspace --all-targets $(CARGO_FEATURES) -- -D warnings

cache-stats:
	@if command -v sccache >/dev/null 2>&1; then \
		sccache --show-stats; \
	else \
		echo "sccache is not installed; run: cargo install sccache"; \
	fi

# Fetch open-dataset sample parquet files into local/samples/ for local testing.
samples:
	./scripts/fetch_samples.sh

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

check: fmt-check lint isolation lfs-check test

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

# python/ is its own crate (not a workspace member); share the repo target
# dir so it does not grow python/target. Local to python/.venv for PEP 668.
check-python:
	$(CARGO) fmt --manifest-path python/Cargo.toml -- --check
	CARGO_TARGET_DIR="$(or $(CARGO_TARGET_DIR),$(CURDIR)/target)" $(CARGO) clippy --manifest-path python/Cargo.toml --all-targets -- -D warnings
	test -d python/.venv || $(PYTHON) -m venv python/.venv
	python/.venv/bin/python -m pip install -q maturin
	. python/.venv/bin/activate && CARGO_TARGET_DIR="$(or $(CARGO_TARGET_DIR),$(CURDIR)/target)" maturin develop --manifest-path python/Cargo.toml
	python/.venv/bin/python -m unittest discover -s python/tests

clean:
	$(CARGO) clean
