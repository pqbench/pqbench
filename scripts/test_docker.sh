#!/bin/sh
# Smoke-test the pqbench container image.
#
# What it verifies (end to end, on the built image):
#   1. the image runs as the non-root user the Dockerfile declares,
#   2. `pqbench --help` runs and lists the subcommands,
#   3. `bytemass` reads a parquet file's metadata and emits a well-formed
#      byte-mass JSON tree (works on any parquet, compressed or not),
#   4. `lz` sweeps the file and reports every wired codec (gzip, lz4, snappy,
#      zstd) — proving the statically linked codecs shipped in the image.
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

echo "== smoke-testing $image (runtime: $runtime) =="

echo "checking the image runs as non-root user 65532:65532..."
u=$("$runtime" image inspect --format '{{.Config.User}}' "$image") \
    || fail "could not inspect image '$image'"
[ "$u" = "65532:65532" ] || fail "image user is '$u', expected 65532:65532"
ok "image user is 65532:65532"

echo "checking 'pqbench --help' runs and lists subcommands..."
"$runtime" run --rm --network none --read-only "$image" --help > "$scratch/help" \
    || fail "'--help' exited non-zero"
grep -q bytemass "$scratch/help" \
    || fail "'--help' output does not mention 'bytemass'"
ok "'--help' mentions bytemass"

echo "checking 'bytemass --json' parses a parquet file..."
"$runtime" run --rm --network none --read-only \
    -v "$root/crates/pqbench/tests/fixtures:/data:ro" \
    "$image" bytemass /data/small_snappy.parquet --json > "$scratch/mass.json" \
    || fail "'bytemass --json' exited non-zero"
python3 - "$scratch/mass.json" <<'PY'
import json
import sys
with open(sys.argv[1]) as stream:
    tree = json.load(stream)
assert tree["name"] == "small_snappy.parquet", f"name is {tree.get('name')!r}"
assert tree["value"] > 0, f"value is {tree.get('value')!r}"
assert tree["children"], "no children in tree"
PY
ok "bytemass produced a valid byte-mass JSON tree"

echo "checking 'lz' sweeps every wired codec..."
"$runtime" run --rm --network none --read-only \
    -v "$root:/data:ro" "$image" lz /data/README.md \
    --samples 1 --warmup-iterations 0 > "$scratch/lz" \
    || fail "'lz' exited non-zero"
for codec in gzip lz4 snappy zstd; do
    grep -q "$codec" "$scratch/lz" \
        || fail "codec '$codec' is missing from 'lz' output"
    ok "codec present: $codec"
done

echo "PASS: all smoke tests passed"
