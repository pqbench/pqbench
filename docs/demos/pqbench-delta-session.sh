#!/bin/sh
set -eu

pqbench() {
    "${PQBENCH:?set PQBENCH to the pqbench binary}" "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

prompt "pqbench table ls docker/e2e-lakehouse/table -o /tmp/pqbench-demo-partition.ndjson.zst"
pqbench table ls docker/e2e-lakehouse/table -o /tmp/pqbench-demo-partition.ndjson.zst
sleep 2

prompt "pqbench table ls docker/e2e-lakehouse/table | pqbench partition ls | pqbench bytemass"
pqbench table ls docker/e2e-lakehouse/table | pqbench partition ls | pqbench bytemass | cat
sleep 2
