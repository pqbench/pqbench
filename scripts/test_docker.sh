#!/bin/sh
# Smoke-test the pqbench container image.
#
# What it verifies (end to end, on the built image):
#   1. the image runs as the non-root user the Dockerfile declares,
#   2. `pqbench --help` runs and lists the subcommands,
#   3. `bytemass` reads a parquet file's metadata and emits a well-formed
#      flat per-column JSON report (works on any parquet, compressed or not),
#   4. `lz` sweeps the file and reports every wired codec (gzip, lz4, snappy,
#      zstd) — proving the statically linked codecs shipped in the image.
#   5. `compression` runs on a NONE-compressed parquet and reports every codec.
#
# It deliberately uses the checked-in fixture (not the untracked data/bench
# files), runs with --network none, and needs no credentials, so it can run on
# any host or in CI.
#
# Default runtime is Docker; override with RUNTIME=podman (or any
# docker-compatible CLI).
set -eu

runtime=${RUNTIME:-docker}
image=${1:-pqbench:local}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

fail() { echo "FAIL: $*" >&2; exit 1; }
ok()   { echo "ok:   $*"; }

run_image() { "$runtime" run --rm --network none --read-only "$@"; }

# check_codecs <output_file> <command_name>
check_codecs() {
    for codec in gzip lz4 snappy zstd; do
        grep -q "$codec" "$1" \
            || fail "codec '$codec' is missing from '$2' output"
        ok "codec present: $codec"
    done
}

echo "== smoke-testing $image (runtime: $runtime) =="

echo "checking the image runs as non-root user 65532:65532..."
u=$("$runtime" image inspect --format '{{.Config.User}}' "$image") \
    || fail "could not inspect image '$image'"
[ "$u" = "65532:65532" ] || fail "image user is '$u', expected 65532:65532"
ok "image user is 65532:65532"

echo "checking 'pqbench --help' runs and lists subcommands..."
run_image "$image" --help > "$scratch/help" \
    || fail "'--help' exited non-zero"
grep -q bytemass "$scratch/help" \
    || fail "'--help' output does not mention 'bytemass'"
ok "'--help' mentions bytemass"

echo "checking 'bytemass --json' parses a parquet file..."
run_image -v "$root/crates/pqbench/tests/fixtures:/data:ro" \
    "$image" bytemass /data/small_snappy.parquet --json > "$scratch/mass.json" \
    || fail "'bytemass --json' exited non-zero"
python3 - "$scratch/mass.json" <<'PY'
import json
import sys
with open(sys.argv[1]) as stream:
    report = json.load(stream)
assert report["file_count"] == 1, f"file_count is {report.get('file_count')!r}"
assert report["num_rows"] > 0, f"num_rows is {report.get('num_rows')!r}"
assert report["columns"], "no columns in report"
assert all("path" in column and "compressed_bytes" in column for column in report["columns"])
PY
ok "bytemass produced a valid byte-mass JSON report"

echo "checking 'lz' sweeps every wired codec..."
run_image -v "$root:/data:ro" "$image" lz /data/README.md \
    --samples 1 --warmup-iterations 0 > "$scratch/lz" \
    || fail "'lz' exited non-zero"
check_codecs "$scratch/lz" "lz"

echo "checking 'compression' runs on a NONE-compressed parquet..."
run_image -v "$root/crates/pqbench/tests/fixtures:/data:ro" \
    "$image" compression /data/small_reddit_none.parquet \
    --samples 1 --warmup-iterations 0 > "$scratch/comp" \
    || fail "'compression' exited non-zero"
check_codecs "$scratch/comp" "compression"
ok "compression produced a codec sweep"

echo "PASS: all smoke tests passed"
