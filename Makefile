# Standard Rust workspace developer/orchestration targets.
# Overridable without modifying this file: CARGO (default: cargo).
# Example: make CARGO=cargo+  build

CARGO ?= cargo
LAKEHOUSE = CARGO="$(CARGO)" ./docker/e2e-lakehouse/lakehouse.sh

.PHONY: all fmt fmt-check build test lint cache-stats samples e2e lakehouse \
	lakehouse-up lakehouse-seed-s3 lakehouse-seed-unity check clean

all: fmt build test lint

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

build:
	$(CARGO) build --workspace

test:
	$(CARGO) test --workspace

lint:
	$(CARGO) clippy --workspace --all-targets -- -D warnings

cache-stats:
	@if command -v sccache >/dev/null 2>&1; then \
		sccache --show-stats; \
	else \
		echo "sccache is not installed; run: cargo install sccache"; \
	fi

# Fetch open-dataset sample parquet files into local/samples/ for local testing.
samples:
	./scripts/fetch_samples.sh

# Network e2e (ignored by default): measure a public S3 object anonymously.
e2e:
	$(CARGO) test -p pqbench --features aws --test redset_e2e -- --ignored

# Local Unity Catalog, ready to query: see docker/e2e-lakehouse/README.md.
lakehouse: lakehouse-seed-s3 lakehouse-seed-unity
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

check: fmt-check lint test

clean:
	$(CARGO) clean
