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

.PHONY: all fmt fmt-check build test lint cache-stats samples lakehouse dbx-e2e \
	lakehouse-up lakehouse-seed-s3 lakehouse-seed-unity lakehouse-seed-iceberg \
	check isolation lfs-check check-python sync-docs check-docs \
	check-contracts update-contracts clean

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

check: fmt-check check-docs lint isolation lfs-check test check-contracts

# Compare public CLI JSON shapes with the committed snapshot. A deliberate
# output change starts with `make update-contracts` and a change-log entry.
check-contracts:
	$(PYTHON) -m unittest scripts.test_check_io_contracts
	$(CARGO) build -q -p pqbench-cli $(CARGO_FEATURES)
	$(PYTHON) scripts/check_io_contracts.py --binary "$(or $(CARGO_TARGET_DIR),target)/debug/pqbench"

update-contracts:
	$(CARGO) build -q -p pqbench-cli $(CARGO_FEATURES)
	$(PYTHON) scripts/check_io_contracts.py --binary "$(or $(CARGO_TARGET_DIR),target)/debug/pqbench" --update

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

# Turn the documented `pqbench ...` commands into Rust integration tests under
# crates/pqbench-cli/tests/gen_*.rs. Committed, so `make test` runs them like any
# other test; `make check-docs` fails if they are out of date with the Markdown.
DOCS ?= README.md docs/ skills/
sync-docs:
	$(CARGO) run -q -p docscheck-cli -- sync $(DOCS)
check-docs:
	$(CARGO) run -q -p docscheck-cli -- check $(DOCS)

clean:
	$(CARGO) clean
