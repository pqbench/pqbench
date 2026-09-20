#!/usr/bin/env bash
# Leave the stand available for interactive experiments.
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
compose() { docker compose -f "$root/docker/e2e-lakehouse/compose.yaml" "$@"; }
assert_events_table() {
    python3 -c 'import json,sys; r=json.load(sys.stdin); assert r["num_rows"] == 3; assert {c["path"] for c in r["columns"]} == {"id", "label"}; assert all(c["compressed_bytes"] > 0 for c in r["columns"])'
}
cargo build -p pqbench-cli --features aws
compose build examples
# Storage first: Unity is configured with a credential rustfs mints here, so it
# has something to vend by the time it starts.
compose up -d --wait rustfs
eval "$(compose run --rm -T examples credentials)"
export VENDED_ACCESS_KEY_ID VENDED_SECRET_ACCESS_KEY VENDED_SESSION_TOKEN
compose up -d --wait
compose run --rm -T examples seed

# Unity vends the credentials in its document, so this pipe needs none itself.
compose run --rm -T examples unity |
    "$root/target/debug/pqbench" bytemass --source - --json |
    assert_events_table
echo "PASS: unity credential vending → source JSON → pqbench S3 footer reads"

# The other two name objects only; pqbench reads S3 the usual way, from the
# environment.
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
export AWS_ENDPOINT="http://localhost:${RUSTFS_PORT:-9000}"
export AWS_ALLOW_HTTP=true AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
for engine in iceberg ducklake; do
    compose run --rm -T examples "$engine" |
        "$root/target/debug/pqbench" bytemass --source - --json |
        assert_events_table
    echo "PASS: $engine catalog → source JSON → pqbench S3 footer reads"
done
