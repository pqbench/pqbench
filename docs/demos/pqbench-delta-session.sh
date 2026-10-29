#!/bin/sh
set -eu

pqbench() {
    "${PQBENCH:?set PQBENCH to the pqbench binary}" "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

prompt "cat docker/e2e-lakehouse/table.ndjson | pqbench list files | head -3"
cat docker/e2e-lakehouse/table.ndjson | pqbench list files | head -3
sleep 2

prompt "cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench bytemass"
cat docker/e2e-lakehouse/table.ndjson | pqbench list files | pqbench bytemass | cat
sleep 2
