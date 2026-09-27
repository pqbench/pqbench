#!/bin/sh
# Lists the committed Iceberg fixture. Measuring it needs rustfs + iceberg-s3
# because the manifests name s3://lakehouse/... — run `make lakehouse` first.
set -eu

pqbench() {
    "${PQBENCH:?set PQBENCH to the pqbench binary}" "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

prompt "pqbench lake docker/e2e-lakehouse/iceberg -o /tmp/pqbench-demo-iceberg.ndjson.zst"
pqbench lake docker/e2e-lakehouse/iceberg -o /tmp/pqbench-demo-iceberg.ndjson.zst
sleep 2

if [ "${PQBENCH_LAKEHOUSE:-}" = "1" ]; then
    prompt "pqbench lake docs/demos/iceberg-rest.json | pqbench table | pqbench bytemass --json"
    pqbench lake docs/demos/iceberg-rest.json | pqbench table | pqbench bytemass --json | cat
    sleep 2
fi
