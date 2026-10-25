#!/bin/sh
set -eu

pqbench() {
    "${PQBENCH:?set PQBENCH to the pqbench binary}" "$@"
}

prompt() {
    printf '\033[1;32m$\033[0m %s\n' "$*"
    sleep 1
}

prompt "pqbench list partitions docker/e2e-lakehouse/table | head -3"
pqbench list partitions docker/e2e-lakehouse/table | head -3
sleep 2

prompt "pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass"
pqbench list partitions docker/e2e-lakehouse/table | pqbench list files | pqbench bytemass | cat
sleep 2
