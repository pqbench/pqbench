# Standard Rust workspace developer/orchestration targets.
# Overridable without modifying this file: CARGO (default: cargo).
# Example: make CARGO=cargo+  build

CARGO ?= cargo

.PHONY: all fmt fmt-check build test lint samples check clean

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

# Fetch open-dataset sample parquet files into local/samples/ for local testing.
samples:
	./scripts/fetch_samples.sh

check: fmt-check lint test

clean:
	$(CARGO) clean