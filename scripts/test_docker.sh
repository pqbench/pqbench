#!/bin/sh
# Run on either supported architecture; no remote datasets or credentials.
set -eu

image=${1:-pqbench:local}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT HUP INT TERM

test "$(docker image inspect --format '{{.Config.User}}' "$image")" = '65532:65532'
docker run --rm --network none --read-only "$image" --help > "$scratch/help"
grep -q bytemass "$scratch/help"

docker run --rm --network none --read-only \
    -v "$root/crates/pqbench/tests/fixtures:/data:ro" \
    "$image" bytemass /data/small_snappy.parquet --json > "$scratch/mass.json"
python3 - "$scratch/mass.json" <<'PY'
import json
import sys

with open(sys.argv[1]) as stream:
    tree = json.load(stream)
assert tree["name"] == "small_snappy.parquet", tree
assert tree["value"] > 0, tree
assert tree["children"], tree
PY

# Exercise each linked native codec in the runtime image.
docker run --rm --network none --read-only \
    -v "$root:/data:ro" "$image" lz /data/README.md \
    --samples 1 --warmup-iterations 0 > "$scratch/lz"
for codec in gzip lz4 snappy zstd; do
    grep -q "$codec" "$scratch/lz"
done
