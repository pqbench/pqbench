#!/bin/sh
# Runs the lake pipe this branch ships. Catalog / s3:// walks need the stand
# (`make lakehouse`) and are not substituted with jq of a fixture.
set -eu

pqbench() {
    "${PQBENCH:?set PQBENCH to the pqbench binary}" "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

prompt "pqbench lake docker/e2e-lakehouse -o /tmp/pqbench-demo-lake.ndjson.zst"
pqbench lake docker/e2e-lakehouse -o /tmp/pqbench-demo-lake.ndjson.zst
sleep 2

prompt "pqbench lake docs/demos/lake.json | pqbench table | pqbench bytemass --json"
pqbench lake docs/demos/lake.json | pqbench table | pqbench bytemass --json | cat
sleep 2
