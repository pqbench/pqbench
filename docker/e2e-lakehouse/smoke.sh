#!/usr/bin/env bash
# Leave the stand available for interactive experiments.
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
compose() { docker compose -f "$root/docker/e2e-lakehouse/compose.yaml" "$@"; }
# pqbench reads S3 the usual way: configuration comes from the environment.
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
export AWS_ENDPOINT="http://localhost:${RUSTFS_PORT:-9000}"
export AWS_ALLOW_HTTP=true AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
cargo build -p pqbench-cli --features aws
compose build examples
compose up -d --wait
compose run --rm -T examples seed
for engine in unity iceberg ducklake; do
    compose run --rm -T examples "$engine" |
        "$root/target/debug/pqbench" bytemass --source - --json |
        python3 -c 'import json,sys; r=json.load(sys.stdin); assert r["value"] > 0; assert {c["name"] for c in r["children"]} == {"id", "label"}'
    echo "PASS: $engine catalog → source JSON → pqbench S3 footer reads"
done
