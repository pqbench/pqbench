#!/bin/sh
set -eu

pqbench() {
    "${PQBENCH:?set PQBENCH to the pqbench binary}" "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

prompt "pqbench table docker/e2e-lakehouse/table"
pqbench table docker/e2e-lakehouse/table
sleep 2

prompt "pqbench table docker/e2e-lakehouse/table | pqbench bytemass"
pqbench table docker/e2e-lakehouse/table | pqbench bytemass
sleep 2
