# Standard Rust workspace developer/orchestration targets.
# Overridable without modifying this file:
#   CARGO          (default: cargo)
#   CARGO_FEATURES (default: --all-features) feature selection; override only to
#                  reproduce a specific CI leg, e.g. CARGO_FEATURES=--features aws
#   TEST_FLAGS     (default: empty) test-harness args after `--`, e.g. --include-ignored
# Examples:
#   make check
#   make test TEST_FLAGS=--include-ignored

CARGO ?= cargo
CARGO_FEATURES ?= --all-features
TEST_FLAGS ?=
PYTHON ?= python3

# Share one target dir across linked worktrees. The pre-commit hook runs
# `make check`, and a fresh worktree would otherwise recompile the whole tree
# (and link the multi-GB test binaries) into its own target/ — the memory spike
# that gets OOM-killed. `--git-common-dir` is the main checkout's .git in both
# the main checkout and a linked worktree, so its parent is the target to
# share. An explicit CARGO_TARGET_DIR from the environment wins.
GIT_COMMON_DIR := $(shell git rev-parse --path-format=absolute --git-common-dir 2>/dev/null)
MAIN_CHECKOUT := $(patsubst %/.git,%,$(GIT_COMMON_DIR))
CARGO_TARGET_DIR ?= $(if $(MAIN_CHECKOUT),$(MAIN_CHECKOUT)/target)
ifneq ($(CARGO_TARGET_DIR),)
export CARGO_TARGET_DIR
endif

# Cap parallel rustc/link jobs by default: the test binaries link several GB
# each, and a full rebuild at one job per core exhausts a 16 GB machine. Raise
# it (`CARGO_BUILD_JOBS=8 make check`) when the memory is there; CI sets its own.
CARGO_BUILD_JOBS ?= 2
export CARGO_BUILD_JOBS

# Cargo locks only the profile dir (target/debug/.cargo-lock), so a debug gate
# and a release build can link at the same time. Serialize every heavy
# invocation on one lock in the shared git dir; `flock` is not present on every
# platform (macOS), so run unlocked there rather than fail.
FLOCK := $(shell command -v flock 2>/dev/null)
CARGO_LOCK ?= $(if $(GIT_COMMON_DIR),$(GIT_COMMON_DIR)/pqbench-cargo.lock,/tmp/pqbench-cargo.lock)
CARGO_LOCKED = $(if $(FLOCK),$(FLOCK) $(CARGO_LOCK),) $(CARGO)
# rust-code-analysis CLI and inputs; see the `code-analysis` target.
RCA ?= rust-code-analysis-cli
CODE_ANALYSIS_PATHS ?= crates
LAKEHOUSE = CARGO="$(CARGO)" ./docker/e2e-lakehouse/lakehouse.sh

# Run recipes under bash so `time` can use TIMEFORMAT (the default /bin/sh has
# no portable millisecond format). Each timed target prints `<name>: N s`.
SHELL := /bin/bash

.PHONY: all fmt fmt-check build test lint cache-stats cargo link-target samples lakehouse dbx-e2e \
	dbx-walk lakehouse-up lakehouse-seed-s3 lakehouse-seed-unity \
	lakehouse-seed-iceberg check check-fast isolation lfs-check check-python sync-docs \
	check-docs code-analysis test-code-analysis clean

all: fmt build test lint

fmt:
	@TIMEFORMAT='fmt: %3R s'; time $(CARGO) fmt --all

fmt-check:
	@TIMEFORMAT='fmt-check: %3R s'; time $(CARGO) fmt --all -- --check

build:
	@TIMEFORMAT='build: %3R s'; time $(CARGO_LOCKED) build --workspace $(CARGO_FEATURES)

test:
	@TIMEFORMAT='test: %3R s'; time $(CARGO_LOCKED) test --workspace $(CARGO_FEATURES) $(if $(TEST_FLAGS),-- $(TEST_FLAGS))

# The live Databricks metastore e2e; needs the service principal credentials.
# The S3 features are on so the walk's storage reads actually run (under the
# vended keys; Databricks default storage refuses externally issued sessions).
dbx-e2e:
	@test -n "$$DBX_HOST" && test -n "$$DBX_SAMPLES_SP_CLIENT_ID" || { echo "set DBX_HOST and DBX_SAMPLES_SP_CLIENT_ID/SECRET (see docs/guides/auth.md)"; exit 1; }
	$(CARGO_LOCKED) test -p pqbench-cli --features delta-s3,iceberg-s3 --test it -- dbx_e2e --ignored --nocapture

# The live walk (scripts/dbx-walk.sh): one pass over the whole Unity Catalog,
# from `list catalogs` to the `format --html` page. Needs DBX_HOST plus a bearer
# source — the service principal's client secret, or a ready PQB_ENDPOINT /
# PQB_TOKEN. The `dbx-walk` CI job runs it as a live smoke test. A debug binary
# is enough — the walk is paced, not compute-bound — so no `--release`; build
# one and point PQB at it to profile with `perf` (docs/guides/performance.md).
dbx-walk:
	@test -n "$$DBX_HOST" || { echo "set DBX_HOST (see docs/guides/auth.md)"; exit 1; }
	$(CARGO_LOCKED) build -p pqbench-cli $(CARGO_FEATURES)
	PQB="$(or $(CARGO_TARGET_DIR),$(CURDIR)/target)/debug/pqbench" ./scripts/dbx-walk.sh

lint:
	@TIMEFORMAT='lint: %3R s'; time $(CARGO_LOCKED) clippy --workspace --all-targets $(CARGO_FEATURES) -- -D warnings

cache-stats:
	@if command -v sccache >/dev/null 2>&1; then \
		sccache --show-stats; \
	else \
		echo "sccache is not installed; run: cargo install sccache"; \
	fi

# Run cargo through the shared target dir and the job cap:
# make cargo CMD="check -p pqbench --all-features".
cargo:
	$(CARGO_LOCKED) $(CMD)

# A linked worktree's bare `cargo` writes to ./target, not the shared root.
# Point it at the main checkout so every command reuses one artifact tree.
# Refuses to touch an existing target/ unless FORCE=1 (that deletes its cache).
link-target:
	@if [ -z "$(MAIN_CHECKOUT)" ]; then echo "not a git checkout" >&2; exit 1; fi; \
	if [ "$(CURDIR)" = "$(MAIN_CHECKOUT)" ]; then echo "main checkout: nothing to do"; exit 0; fi; \
	if [ -L target ]; then echo "target -> $$(readlink target)"; exit 0; fi; \
	if [ -e target ] && [ "$(FORCE)" != 1 ]; then \
		echo "target/ exists; rerun with FORCE=1 to replace it" >&2; exit 1; fi; \
	rm -rf target && ln -s "$(CARGO_TARGET_DIR)" target && echo "target -> $(CARGO_TARGET_DIR)"

# Verify the committed open-dataset parquet samples in data/samples/ (Git LFS).
samples:
	@TIMEFORMAT='samples: %3R s'; time ./scripts/check_samples.sh

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

check: fmt-check check-docs lint isolation lfs-check test test-code-analysis

# Fast gate for iteration: type-check every target (no codegen, no links).
# Run the full `make check` once before pushing.
check-fast: fmt-check check-docs
	@TIMEFORMAT='check: %3R s'; time $(CARGO_LOCKED) check --workspace --all-targets $(CARGO_FEATURES)

# `third_party` wrappers keep feature flags in impl.rs, never in api.rs.
# ISOLATION_FLAGS=--github renders GitHub workflow-command annotations.
ISOLATION_FLAGS ?=
isolation:
	@TIMEFORMAT='isolation: %3R s'; time ./scripts/check_isolation.sh $(ISOLATION_FLAGS)

# Every file under an LFS filter must be committed as a pointer, not a raw git
# blob. A raw blob checks out (the smudge filter only warns) but never reaches
# the LFS server, so it is invisible until a fresh clone. git-lfs is a
# contributor prerequisite; see CONTRIBUTING.md.
lfs-check:
	@TIMEFORMAT='lfs-check: %3R s'; time git lfs fsck

# The wheel is data-only: maturin packages the `pqbench` binary from
# crates/pqbench-cli in the wheel's `.data/scripts/` (bindings = "bin"), so pip
# installs it on PATH. No PyO3, no libpython, no ABI. This builds the debug
# profile: the check validates packaging (bin bindings, scripts dir, install,
# smoke), and the release build is the one the release workflow ships. Set
# PQB_WHEEL_PROFILE=release to validate the shipped profile locally. Local to
# python/.venv for PEP 668.
PQB_WHEEL_PROFILE ?= dev

check-python:
	test -d python/.venv || $(PYTHON) -m venv python/.venv
	python/.venv/bin/python -m pip install -q maturin
	rm -f python/dist/*.whl
	CARGO_TARGET_DIR="$(or $(CARGO_TARGET_DIR),$(CURDIR)/target)" \
		python/.venv/bin/maturin build --profile $(PQB_WHEEL_PROFILE) --out python/dist
	python/.venv/bin/python -m pip install -q --force-reinstall python/dist/*.whl
	python/.venv/bin/pqbench --help > /dev/null
	set -o pipefail; python/.venv/bin/pqbench table-file examples/quickstart.parquet \
		| python/.venv/bin/pqbench bytemass > /dev/null

# Two generated artifacts, both committed:
# - `crates/pqbench-cli/tests/gen_*.rs`, from the documented `pqbench ...`
#   commands; `make test` runs them like any other integration test.
# - `docs/reference/cli.md`, from the clap tree via `crates/docsgen`.
# `make check-docs` fails if either is out of date with its source.
DOCS ?= README.md docs/ skills/ examples/
sync-docs:
	@TIMEFORMAT='sync-docs: %3R s'; time $(CARGO_LOCKED) run -q -p docscheck-cli -- sync $(DOCS)
	$(CARGO_LOCKED) run -q -p docsgen -- write docs/reference/cli.md
check-docs:
	@TIMEFORMAT='check-docs: %3R s'; time $(CARGO_LOCKED) run -q -p docscheck-cli -- check $(DOCS)
	$(CARGO_LOCKED) run -q -p docsgen -- check docs/reference/cli.md

# Software metrics (rust-code-analysis) over the source tree; prints the CLI's
# own metrics tree. Needs `rust-code-analysis-cli` on PATH — CI pins the
# release — or set RCA to a binary. Report-only: `make check` does not run it.
code-analysis:
	$(RCA) --metrics -p $(CODE_ANALYSIS_PATHS)

# The reporter's tests do not need rust-code-analysis: they feed it the per-file
# JSON documents the CLI writes.
test-code-analysis:
	@TIMEFORMAT='test-code-analysis: %3R s'; time $(PYTHON) -m unittest discover -s scripts -p 'test_*.py'

clean:
	@if [ -n "$(MAIN_CHECKOUT)" ] && [ "$(MAIN_CHECKOUT)" != "$(CURDIR)" ]; then \
		echo "clean: refusing to wipe the shared target dir $(CARGO_TARGET_DIR) from a worktree" >&2; \
		exit 1; \
	fi
	$(CARGO) clean
