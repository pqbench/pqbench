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

.PHONY: all fmt fmt-check build test lint cache-stats samples check clean

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

check: fmt-check lint test

clean:
	$(CARGO) clean
